import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def upload_to_gcs_with_gsutil(bucket_name, src_file, dst_file):
    # gsutil 명령어를 사용하여 파일을 GCS에 업로드
    command = f"gsutil cp {src_file} gs://{bucket_name}/{dst_file}"
    subprocess.run(command, shell=True, check=True)

with DAG(
    'json_to_csv_to_gcs_with_gsutil_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 12, 9),
        'retries': 1,
    },
    description='Convert JSON to CSV and upload to GCS using gsutil',
    schedule_interval=None,
) as dag:

    json_url = 'https://example.com/data.json'
    output_file = '/tmp/output.csv'
    bucket_name = 'your-gcs-bucket-name'
    dst_file = 'output.csv'

    # JSON을 CSV로 변환하는 작업
    convert_json_to_csv = PythonOperator(
        task_id='convert_json_to_csv',
        python_callable=json_to_csv,
        op_args=[json_url, output_file],
    )

    # gsutil을 사용하여 GCS에 업로드하는 작업
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs_with_gsutil',
        python_callable=upload_to_gcs_with_gsutil,
        op_args=[bucket_name, output_file, dst_file],
    )

    # DAG 작업 순서 설정
    convert_json_to_csv >> upload_to_gcs_task
