from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests

def fetch_api_data():
    url = "http://www.khoa.go.kr/api/oceangrid/ObsServiceObj/search.do?ServiceKey=wldhxng34hkddbsgm81lwldhxng34hkddbsgm81l==&ResultType=json"
    response = requests.get(url)
    if response.status_code == 200:
        print("API Response:", response.text)
        return response.text
    else:
        raise Exception(f"API request failed with status {response.status_code}")

with DAG(
    dag_id='api_fetch_dag',
    default_args={
        'owner': 'airflow',
    },
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_api_task = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )
