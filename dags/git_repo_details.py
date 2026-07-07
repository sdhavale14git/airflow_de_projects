import os
import csv
import logging
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import timedelta, datetime

config_variables = Variable.get("github_repo_fetch_var", deserialize_json=True)
url = config_variables.get("url", "https://api.github.com/search/repositories")
params = config_variables.get("params")

#config
# url = "https://api.github.com/search/repositories"

# params = {
#     "q": "language:python created:>2025-04-22",
#     "sort": "stars",
#     "order": "desc",
#     "per_page": 30
# }

# define some variables as airflow variables.

# Initiating the default_args
default_args = {
    'owner' : 'airflow',
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'start_date' : datetime(2022, 11, 12)
}

# Creating DAG Object
dag = DAG(dag_id = 'git_repo_details',
    default_args = default_args,
    schedule_interval = None, 
    catchup=False,
    tags=['git_details'],
    )

#first task
dag_start = DummyOperator(task_id = 'dag_start', dag = dag)

#last task
dag_end = DummyOperator(task_id = 'dag_end', dag = dag)

def api_call(**kwargs):
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()  # Raise an error for bad responses
    data = response.json()
    print(response.status_code)
    print(data.keys())
    return data

extract_task = PythonOperator(
    task_id = 'extract_task',
    python_callable = api_call,
    dag = dag
)

def create_dataframe(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_task')     # <-- pull data from extract_task
    repos = []
    for repo in data['items']:
        repos.append({
            "name": repo['name'],
            "owner": repo['owner']['login'],
            "stars": repo['stargazers_count'],
            "forks": repo['forks_count'],
            "language": repo['language'],
            "description": repo['description'],
            "url": repo['html_url'],
            "created_at": repo['created_at']
        })
    df = pd.DataFrame(repos)
    return df

df_creation_task = PythonOperator(
    task_id = 'df_creation_task',
    python_callable = create_dataframe,
    dag = dag
)

def create_csv(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='df_creation_task')  # <-- pull data from df_creation_task

    df_clean = df.dropna(subset=['description'])      # Drop rows where description is missing
    df_clean = df_clean.copy()
    df_clean['viral'] = df_clean['stars'].apply(lambda x: 'Yes' if x > 50000 else 'No')
    df_clean = df_clean.sort_values('stars', ascending=False).reset_index(drop=True)
    print("Before cleaning:", len(df))
    print("After cleaning:", len(df_clean))
    csv_path = os.path.join(os.path.dirname(__file__), 'github_trending_repos.csv')
    df_clean.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL, encoding='utf-8')
    return df_clean

csv_creation_task = PythonOperator(
    task_id = 'csv_creation_task',
    python_callable = create_csv,
    dag = dag
)

def filter_existing_rows(**kwargs):
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="csv_creation_task")

    if df is None:
        return pd.DataFrame()

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.github_trending_repos (
            name TEXT,
            owner TEXT,
            stars INTEGER,
            forks INTEGER,
            language TEXT,
            description TEXT,
            url TEXT,
            created_at TIMESTAMP,
            viral TEXT
        )
    """)
    conn.commit()

    cursor.execute("SELECT url FROM public.github_trending_repos WHERE url IS NOT NULL")
    existing_urls = {row[0] for row in cursor.fetchall()}
    cursor.close()
    conn.close()

    df = df.dropna(subset=["url"]).copy()
    df_new = df[~df["url"].isin(existing_urls)].reset_index(drop=True)

    csv_path = os.path.join(os.path.dirname(__file__), "github_trending_repos_new.csv")
    df_new.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8")

    return df_new


filter_existing_rows_task = PythonOperator(
    task_id="filter_existing_rows_task",
    python_callable=filter_existing_rows,
    dag=dag
)

def load_to_database(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='filter_existing_rows_task')  # <-- pull data from filter_existing_rows_task
    csv_path = os.path.join(os.path.dirname(__file__), 'github_trending_repos_new.csv')

    # Load the CSV file into a PostgreSQL database
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS github_trending_repos (
            name TEXT,
            owner TEXT,
            stars INTEGER,
            forks INTEGER,
            language TEXT,
            description TEXT,
            url TEXT,
            created_at TIMESTAMP,
            viral TEXT
        )
    """)
    conn.commit()

    csv_path = os.path.join(os.path.dirname(__file__), 'github_trending_repos_new.csv')
    
    # Load data from CSV into the database
    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        cursor.copy_expert(
            sql="""
                COPY github_trending_repos (
                    name, owner, stars, forks, language, description, url, created_at, viral
                )
                FROM STDIN WITH CSV HEADER
            """,
            file=f
        )
    
    conn.commit()
    cursor.close()
    conn.close()
load_task = PythonOperator(
    task_id = 'load_task',
    python_callable = load_to_database,
    dag = dag
)

dag_start >> extract_task >> df_creation_task >> csv_creation_task >> filter_existing_rows_task >>load_task >> dag_end