from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime
import requests
import csv
from airflow.models import Variable
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import ssl

# TLS Adapter to enforce TLSv1.2
class TLSAdapter(HTTPAdapter):
    def __init__(self, tls_version=None, **kwargs):
        self.tls_version = tls_version
        # SSLContext 생성, check_hostname을 False로 설정
        self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.context.check_hostname = False  # 호스트네임 검증 비활성화
        self.context.verify_mode = ssl.CERT_NONE  # 인증서 검증 비활성화
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.context
        return super().init_poolmanager(*args, **kwargs)

# JSON 데이터를 API에서 가져오는 함수
def fetch_json_data(**kwargs):
    # API URL 및 파라미터
    api_url = "https://apis.data.go.kr/1160100/service/GetFPAtmbInsujoinInfoService/getContractInfo"
    params = {
        'pageNo': 1,
        'numOfRows': 10,
        'likeIsuCmpyOfrYm': '202307',
        'isuItmsNm': '개인용',
        'serviceKey': 'im0AFFv2NghkS%2BKEMVEZ43gFtu7hcWRSx9zCUoyu54NmI4kkvJZ2YhVMeuLT0Nisc%2B01BHFPzApF1XJgSFTbeA%3D%3D'
    }

    # TLS 설정된 세션 사용
    session = requests.Session()
    session.mount("https://", TLSAdapter(tls_version=ssl.PROTOCOL_TLSv1_2))

    # API 요청
    response = session.get(api_url, params=params)
    response.raise_for_status()  # 요청 실패 시 예외 발생
    data = response.json()

    # 데이터가 없는 경우 예외 발생
    if 'response' not in data or 'body' not in data['response'] or 'items' not in data['response']['body']:
        raise ValueError("No valid data found in the API response.")

    # 필요한 데이터 추출
    items = data['response']['body']['items']['item']

    # XCom에 데이터 저장
    kwargs['ti'].xcom_push(key='json_data', value=items)

# JSON 데이터를 CSV로 변환하고 GCS에 업로드하는 함수
def json_to_csv_and_upload(**kwargs):
    # XCom에서 JSON 데이터를 가져옴
    json_data = kwargs['ti'].xcom_pull(task_ids='fetch_json_data', key='json_data')

    if not json_data:
        raise ValueError("No data found to convert to CSV.")

    # GCS 버킷 및 파일 설정
    bucket_name = Variable.get("gcs_bucket_csv")
    gcs_file_path = 'kosis_data2.csv'
    local_file_path = '/tmp/kosis_data2.csv'

    # JSON 데이터를 CSV로 변환
    with open(local_file_path, mode='w', newline='', encoding='utf-8-sig') as csv_file:
        csv_writer = csv.writer(csv_file)
        headers = list(json_data[0].keys())
        csv_writer.writerow(headers)  # 헤더 작성

        for item in json_data:
            csv_writer.writerow(item.values())  # 값 작성

    # GCS에 업로드
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    hook.upload(bucket_name, gcs_file_path, local_file_path)
    print(f"File uploaded to GCS: gs://{bucket_name}/{gcs_file_path}")

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG 정의
with DAG(
    dag_id='test_upload',
    default_args=default_args,
    description='Fetch JSON data, convert to CSV, and upload to GCS',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 12, 8),
    catchup=False,
    tags=["session2", "init_test"]
) as dag:

    # JSON 데이터 가져오기
    fetch_json_task = PythonOperator(
        task_id='fetch_json_data',
        python_callable=fetch_json_data,
        provide_context=True,
    )

    # JSON 데이터를 CSV로 변환하고 GCS에 업로드
    json_to_csv_task = PythonOperator(
        task_id='json_to_csv_and_upload',
        python_callable=json_to_csv_and_upload,
        provide_context=True,
    )

    # GCS에 있는 CSV 데이터를 BigQuery에 삽입
    gcs_to_bigquery_task = BigQueryInsertJobOperator(
        task_id='load_csv_to_bigquery',
        configuration={
            "load": {
                "sourceUris": [f"gs://{Variable.get('gcs_bucket_csv')}/kosis_data2.csv"],
                "destinationTable": {
                    "projectId": Variable.get("gcp_project_id"),
                    "datasetId": Variable.get("bigquery_dataset_id"),
                    "tableId": Variable.get("bigquery_table_id_1"),
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "fieldDelimiter": ",",
                "encoding": "UTF-8",
                "autodetect": True,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # 태스크 순서 정의
    fetch_json_task >> json_to_csv_task >> gcs_to_bigquery_task
