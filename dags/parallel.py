from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  # timedelta 임포트

def extract_api():
    print("Extracting data from API...")

def extract_file():
    print("Extracting data from file...")

def extract_db():
    print("Extracting data from database...")

def transform_data(data_source):
    print(f"Transforming data from {data_source}...")

def merge_and_load():
    print("Merging data and loading into target...")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='parallel',
    default_args=default_args,
    start_date=datetime(2023, 12, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 병렬 추출 작업
    extract_api_task = PythonOperator(
        task_id='extract_api',
        python_callable=extract_api,
    )

    extract_file_task = PythonOperator(
        task_id='extract_file',
        python_callable=extract_file,
    )

    extract_db_task = PythonOperator(
        task_id='extract_db',
        python_callable=extract_db,
    )

    # 병렬 변환 작업
    transform_api_task = PythonOperator(
        task_id='transform_api',
        python_callable=lambda: transform_data('API'),
    )

    transform_file_task = PythonOperator(
        task_id='transform_file',
        python_callable=lambda: transform_data('File'),
    )

    transform_db_task = PythonOperator(
        task_id='transform_db',
        python_callable=lambda: transform_data('Database'),
    )

    # 병합 및 로드 작업
    merge_and_load_task = PythonOperator(
        task_id='merge_and_load',
        python_callable=merge_and_load,
    )

    # DAG 의존성 설정
    extract_tasks = [extract_api_task, extract_file_task, extract_db_task]
    transform_tasks = [transform_api_task, transform_file_task, transform_db_task]

    # Extract 단계 >> Transform 단계
    for extract_task, transform_task in zip(extract_tasks, transform_tasks):
        extract_task >> transform_task

    # Transform 단계 >> Merge and Load 단계
    transform_tasks >> merge_and_load_task