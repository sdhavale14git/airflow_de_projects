from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract(**kwargs):
    # Replace URL with your API endpoint
    resp = requests.get('https://jsonplaceholder.typicode.com/posts')
    resp.raise_for_status()
    return resp.json()

def transform(ti):
    data = ti.xcom_pull(task_ids='extract')
    # simple transform: extract id and title
    rows = [(item['id'], item['title']) for item in data]
    ti.xcom_push(key='rows', value=rows)

def load(ti):
    rows = ti.xcom_pull(key='rows', task_ids='transform')
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.executemany('INSERT INTO public.my_table (id, title) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title;', rows)
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='api_to_postgres_example',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023,1,1),
    catchup=False,
    tags=['example']
) as dag:
    t1 = PythonOperator(task_id='extract', python_callable=extract, provide_context=True)
    t2 = PythonOperator(task_id='transform', python_callable=transform)
    t3 = PythonOperator(task_id='load', python_callable=load)

    t1 >> t2 >> t3
