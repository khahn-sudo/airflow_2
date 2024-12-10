import requests
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# JSON 데이터를 CSV로 변환하는 함수
def json_to_csv(json_url, output_file):
    # 주어진 URL에서 JSON 데이터를 가져옴
    response = requests.get(json_url)
    response.raise_for_status()  # 요청 실패 시 예외 발생
    data = response.json()  # JSON 데이터를 파싱

    # JSON 데이터가 리스트 형태인지 확인
    if isinstance(data, list):
        headers = data[0].keys()  # 리스트의 첫 번째 요소에서 헤더 추출
    else:
        raise ValueError("Unexpected JSON structure")  # 데이터 구조가 예상과 다르면 예외 발생

    # CSV 파일로 저장
    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(headers)  # 헤더 작성
        for item in data:
            csv_writer.writerow(item.values())  # 각 데이터 행 작성

# Google Cloud Storage에 파일 업로드 함수
def upload_to_gcs(bucket_name, src_file, dst_file):
    from google.cloud import storage  # GCS 클라이언트 라이브러리

    # GCS 클라이언트 초기화
    client = storage.Client()
    bucket = client.bucket(bucket_name)  # 버킷 가져오기
    blob = bucket.blob(dst_file)  # 업로드할 객체(blob) 생성

    # 로컬 파일을 GCS에 업로드
    blob.upload_from_filename(src_file)
    print(f"Uploaded {src_file} to gs://{bucket_name}/{dst_file}")

# DAG 정의
with DAG(
    'json_to_csv_to_gcs_dag',  # DAG 이름
    default_args={
        'owner': 'airflow',  # 소유자 정보
        'start_date': datetime(2024, 12, 9),  # 시작 날짜 (과거 날짜로 설정)
        'retries': 1,  # 실패 시 재시도 횟수
    },
    description='Convert JSON to CSV and upload to GCS using PythonOperator',  # DAG 설명
    schedule_interval=None,  # 실행 간격 (None이면 수동 실행)
) as dag:

    # JSON 데이터 URL
    json_url = 'https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=NmZhMWQ1MjQ0MDIxZGQ1OGJjYTZkYWFhODhkMmJjOWI=&itmId=13103112873NO_ACCI+13103112873NO_DEATH+13103112873NO_WOUND+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=2016&endPrdDe=2023&orgId=132&tblId=DT_V_MOTA_021'
    
    # 변환된 CSV 파일 경로
    output_file = os.path.join('/tmp', 'output.csv')  # 로컬 파일 저장 경로
    
    # GCS 버킷 및 업로드 대상 파일 경로
    bucket_name = 'kosis_api_test'  # GCS 버킷 이름
    dst_file = 'output.csv'  # GCS에 저장할 파일 이름

    # JSON 데이터를 CSV로 변환하는 작업(Task)
    convert_json_to_csv = PythonOperator(
        task_id='convert_json_to_csv',  # 작업 ID
        python_callable=json_to_csv,  # 실행할 Python 함수
        op_args=[json_url, output_file],  # 함수에 전달할 인수
    )

    # 변환된 CSV 파일을 GCS에 업로드하는 작업(Task)
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',  # 작업 ID
        python_callable=upload_to_gcs,  # 실행할 Python 함수
        op_args=[bucket_name, output_file, dst_file],  # 함수에 전달할 인수
    )

    # 작업 순서 정의: CSV 변환 후 GCS 업로드
    convert_json_to_csv >> upload_to_gcs_task
