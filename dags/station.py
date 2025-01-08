from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json

def print_api_response(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='fetch_api_data')
    print("API Response:", response)

with DAG(
    dag_id='api_fetch_dag',
    default_args={
        'owner': 'airflow',
    },
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_api_data = SimpleHttpOperator(
        task_id='fetch_api_data',
        endpoint='http://www.khoa.go.kr/api/oceangrid/ObsServiceObj/search.do?ServiceKey=wldhxng34hkddbsgm81lwldhxng34hkddbsgm81l==&ResultType=json',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True,
    )

    print_response = PythonOperator(
        task_id='print_response',
        python_callable=print_api_response,
    )

    fetch_api_data >> print_response
