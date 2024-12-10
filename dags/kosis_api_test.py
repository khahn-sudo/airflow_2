import requests
import csv
import io  # 메모리 파일 처리
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

# JSON 데이터를 CSV로 변환하는 함수
def json_to_csv(json_url):
    # 주어진 URL에서 JSON 데이터를 가져옴
    response = requests.get(json_url)
    response.raise_for_status()  # 요청 실패 시 예외 발생
    data = response.json()  # JSON 데이터를 파싱

    # JSON 데이터가 리스트 형태인지 확인
    if isinstance(data, list):
        headers = data[0].keys()  # 리스트의 첫 번째 요소에서 헤더 추출
    else:
        raise ValueError("Unexpected JSON structure")  # 데이터 구조가 예상과 다르면 예외 발생

    # 메모리에서 CSV 데이터를 처리할 수 있는 StringIO 객체 생성
    output_csv = io.StringIO()
    csv_writer = csv.writer(output_csv)
    csv_writer.writerow(headers)  # 헤더 작성
    for item in data:
        csv_writer.writerow(item.values())  # 각 데이터 행 작성

    output_csv.seek(0)  # StringIO의 포인터를 처음으로 이동

    return output_csv.getvalue()  # CSV 데이터를 문자열로 반환

# GCS에 파일 업로드하는 함수
def upload_to_gcs(bucket_name, csv_data, dst_file):
    # GCSHook을 사용하여 Google Cloud Storage에 파일 업로드
    hook = GCSHook(gcp_conn_id="google_cloud_default")  # Conn Id가 google_cloud_default로 설정되어 있으면 그대로 사용
    
    # 문자열을 바이트로 변환하여 GCS로 업로드
    csv_data_bytes = csv_data.encode('utf-8')  # 문자열을 UTF-8로 인코딩하여 바이트로 변환
    
    # GCS에 업로드
    hook.upload(bucket_name, dst_file, csv_data_bytes, mime_type='text/csv')  # 메모리에서 바로 GCS로 업로드
    print(f"Uploaded to gs://{bucket_name}/{dst_file}")

# Airflow Variables에서 설정 가져오기
api_key = Variable.get("kosis_api_key")  # KOSIS API Key 가져오기
bucket_name = Variable.get("gcs_bucket_csv")  # GCS 버킷 이름 가져오기

# DAG 정의
with DAG(
    dag_id="kosis_api_test",
    schedule="0 0 * * *",  # 매일 0시에 실행
    start_date=pendulum.datetime(2024, 12, 8, tz="UTC"),
    catchup=False,
    tags=["session2", "init_test"],
) as dag:

    # JSON 데이터 URL
    json_url = 'https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=NmZhMWQ1MjQ0MDIxZGQ1OGJjYTZkYWFhODhkMmJjOWI=&itmId=13103112873NO_ACCI+13103112873NO_DEATH+13103112873NO_WOUND+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=2017&endPrdDe=2023&orgId=132&tblId=DT_V_MOTA_021'
    
    # GCS에 저장할 파일 이름 (타임스탬프 추가)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dst_file = f'output_{timestamp}.csv'

    # JSON -> CSV 변환 Task
    convert_json_to_csv = PythonOperator(
        task_id='convert_json_to_csv',
        python_callable=json_to_csv,
        op_args=[json_url],
    )

    # GCS에 업로드 Task
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_args=[bucket_name, "{{ task_instance.xcom_pull(task_ids='convert_json_to_csv') }}", dst_file],
    )

    convert_json_to_csv >> upload_to_gcs_task
