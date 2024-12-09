from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import csv
import requests
from datetime import datetime

# JSON 데이터를 CSV로 변환하는 함수
def json_to_csv(json_url, output_file):
    # 1. JSON 데이터를 가져옵니다.
    response = requests.get(json_url)
    data = response.json()

    # 2. CSV로 변환하여 저장합니다.
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["연령층", "월", "사고건수", "사망자수", "부상자수"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)

# Airflow DAG 정의
with DAG(
    'json_to_csv_dag',  # DAG 이름
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 12, 9),
        'retries': 1,
    },
    description='Convert JSON data to CSV',
    schedule_interval=None,  # 수동 실행 (원하는 주기로 설정 가능)
) as dag:

    # PythonOperator를 사용하여 변환 작업 실행
    json_url =   # JSON 파일이 위치한 URL
    output_file = '/path/to/output_test.csv'  # 출력될 CSV 파일 경로

    convert_json_to_csv = PythonOperator(
        task_id='convert_json_to_csv',
        python_callable=json_to_csv,
        op_args=[json_url, output_file],  # json_url과 output_file을 함수에 전달
    )

    # DAG 실행 순서 정의 (여기서는 단일 작업)
    convert_json_to_csv



import requests
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# JSON 데이터를 CSV로 변환하는 함수
def json_to_csv(json_url, output_file):
    response = requests.get(json_url)
    response.raise_for_status()
    data = response.json()

    # CSV로 저장
    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        # 헤더 작성
        headers = data[0].keys()
        csv_writer.writerow(headers)

        # 데이터 작성
        for item in data:
            csv_writer.writerow(item.values())

# Python으로 GCS에 파일 업로드
def upload_to_gcs(bucket_name, src_file, dst_file):
    from google.cloud import storage  # Google Cloud Storage 라이브러리
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dst_file)
    
    # 파일 업로드
    blob.upload_from_filename(src_file)
    print(f"Uploaded {src_file} to gs://{bucket_name}/{dst_file}")

# DAG 정의
with DAG(
    'json_to_csv_to_gcs_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 12, 9),
        'retries': 1,
    },
    description='Convert JSON to CSV and upload to GCS using PythonOperator',
    schedule_interval=None,
) as dag:

    json_url = 'https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=NmZhMWQ1MjQ0MDIxZGQ1OGJjYTZkYWFhODhkMmJjOWI=&itmId=13103112873NO_ACCI+13103112873NO_DEATH+13103112873NO_WOUND+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=2016&endPrdDe=2023&orgId=132&tblId=DT_V_MOTA_021'
    output_file = '/tmp/output.csv'
    bucket_name = 'kosis_api_test'
    dst_file = 'output.csv'

    # JSON -> CSV 변환 Task
    convert_json_to_csv = PythonOperator(
        task_id='convert_json_to_csv',
        python_callable=json_to_csv,
        op_args=[json_url, output_file],
    )

    # GCS 업로드 Task
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_args=[bucket_name, output_file, dst_file],
    )

    # DAG 작업 순서 설정
    convert_json_to_csv >> upload_to_gcs_task
