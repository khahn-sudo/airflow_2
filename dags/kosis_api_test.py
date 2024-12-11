from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from datetime import datetime
import requests
import csv
from airflow.models import Variable

# JSON 데이터를 API에서 가져오는 함수
def fetch_json_data(**kwargs):

    api_key = Variable.get("kosis_api_key")  # Airflow Variable에서 API Key 가져오기
    url = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={api_key}&itmId=13103112873NO_ACCI+13103112873NO_DEATH+13103112873NO_WOUND+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=2017&endPrdDe=2023&orgId=132&tblId=DT_V_MOTA_021"
    
    response = requests.get(url)
    response.raise_for_status()  # 요청 실패 시 예외 발생
    data = response.json()  # JSON 데이터를 파싱
    
    # XCom을 통해 데이터를 반환하여 후속 태스크에서 사용할 수 있게 함
    kwargs['ti'].xcom_push(key='json_data', value=data)

# JSON 데이터를 CSV로 변환하고 GCS에 업로드하는 함수
def json_to_csv_and_upload(**kwargs):
    # XCom에서 JSON 데이터를 가져옴
    json_data = kwargs['ti'].xcom_pull(task_ids='fetch_json_data', key='json_data')

    if not json_data:
        raise ValueError("No data found to convert to CSV.")

    # GCS 버킷 이름 및 파일 경로
    bucket_name = Variable.get("gcs_bucket_csv")  # GCS 버킷 이름
    gcs_file_path = 'kosis_data.csv'
    local_file_path = '/tmp/kosis_data.csv'
    
    # 서비스 계정 키 파일 경로
    service_account_file = '/path/to/your-service-account-file.json'  # 서비스 계정 파일 경로 지정
    scopes = [
        'https://www.googleapis.com/auth/cloud-platform',  # Cloud API 접근 권한
    ]
    
    # 서비스 계정 인증을 통한 인증 객체 생성
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=scopes
    )
    
    # 인증된 GCS Hook 사용
    hook = GCSHook(gcp_conn_id='google_cloud_default', credentials=credentials)

    # JSON 데이터를 CSV로 변환
    with open(local_file_path, mode='w', newline='', encoding='utf-8-sig') as csv_file:
        csv_writer = csv.writer(csv_file)

        # JSON 데이터를 변환할 예시
        headers = list(json_data[0].keys())  # JSON 키를 CSV 헤더로 사용
        csv_writer.writerow(headers)  # 헤더 쓰기

        for item in json_data:
            csv_writer.writerow(item.values())  # 값 쓰기

    # GCS에 업로드
    hook.upload(bucket_name, gcs_file_path, local_file_path)
    print(f"File uploaded to GCS: gs://{bucket_name}/{gcs_file_path}")


# 기본 DAG 설정
default_args = {
    'owner': 'airflow2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# DAG 정의
with DAG(
    'kosis_api_test',  # DAG 이름
    default_args=default_args,
    description='Fetch JSON data, convert to CSV, and upload to GCS',
    schedule_interval='0 0 * * *',  # 매일 0시에 실행
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

    # 태스크 순서 정의
    fetch_json_task >> json_to_csv_task
