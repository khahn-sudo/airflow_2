from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from datetime import datetime
import requests
import csv
from airflow.models import Variable

# JSON 데이터를 API에서 가져오는 함수
def fetch_json_data(**kwargs):
    # API URL 및 파라미터
    api_url = "https://apis.data.go.kr/1160100/service/GetFPAtmbInsujoinInfoService/getContractInfo"
    params = {
        'pageNo': 1,
        'numOfRows': 10,  # 가져올 데이터 수
        'likeIsuCmpyOfrYm': '202307',  # 예시 연월 필터
        'isuItmsNm': '개인용',  # 아이템 이름 필터
        'serviceKey': Variable.get("second_api_service_key"),  # Airflow Variable에서 API Key 가져오기
    }

    # API 요청
    response = requests.get(api_url, params=params)
    response.raise_for_status()  # 요청 실패 시 예외 발생
    data = response.json()

    # 데이터가 없는 경우 예외 발생
    if 'response' not in data or 'body' not in data['response'] or 'items' not in data['response']['body']:
        raise ValueError("No valid data found in the API response.")

    # 필요한 데이터 추출
    items = data['response']['body']['items']['item']  # 'item' 키에서 데이터 추출

    # XCom을 통해 데이터를 반환하여 후속 태스크에서 사용할 수 있게 함
    kwargs['ti'].xcom_push(key='json_data', value=items)


# JSON 데이터를 CSV로 변환하고 GCS에 업로드하는 함수
def json_to_csv_and_upload(**kwargs):
    # XCom에서 JSON 데이터를 가져옴
    json_data = kwargs['ti'].xcom_pull(task_ids='fetch_json_data', key='json_data')

    if not json_data:
        raise ValueError("No data found to convert to CSV.")

    # GCS 버킷 이름 및 파일 경로
    bucket_name = Variable.get("gcs_bucket_csv")  # GCS 버킷 이름
    gcs_file_path = 'kosis_data2.csv'
    local_file_path = '/tmp/kosis_data2.csv'
    
    # JSON 데이터를 CSV로 변환
    with open(local_file_path, mode='w', newline='', encoding='utf-8-sig') as csv_file:
        csv_writer = csv.writer(csv_file)

        # JSON 데이터를 변환할 예시
        headers = list(json_data[0].keys())  # JSON 키를 CSV 헤더로 사용
        csv_writer.writerow(headers)  # 헤더 쓰기

        for item in json_data:
            csv_writer.writerow(item.values())  # 값
