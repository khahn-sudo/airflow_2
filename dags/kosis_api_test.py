from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from datetime import datetime, timedelta
import requests
import csv
from airflow.models import Variable

# JSON 데이터를 API에서 가져오는 함수
def fetch_json_data(**kwargs):
    api_key = Variable.get("kosis_api_key")  # Airflow Variable에서 API Key 가져오기
    url = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={api_key}&itmId=13103112873NO_ACCI+13103112873NO_DEATH+13103112873NO_WOUND+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=2017&endPrdDe=2023&orgId=132&tblId=DT_V_MOTA_021"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
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
    gcs_file_path = 'kosis_data2.csv'
    local_file_path = '/tmp/kosis_data2.csv'
    
    # JSON 데이터를 CSV로 변환
    with open(local_file_path, mode='w', newline='', encoding='utf-8-sig') as csv_file:
        csv_writer = csv.writer(csv_file)

        # JSON 데이터를 변환할 예시
        headers = list(json_data[0].keys())  # JSON 키를 CSV 헤더로 사용
        csv_writer.writerow(headers)  # 헤더 쓰기

        for item in json_data:
            csv_writer.writerow(item.values())  # 값 쓰기

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
    'retries': 1
}

# DAG 정의
with DAG(
    'kosis_upload',  # DAG 이름
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
                    "tableId": Variable.get("bigquery_table_id_2"),
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,  # 헤더 제외
                "writeDisposition": "WRITE_TRUNCATE",  # 기존 데이터 덮어쓰기
                "fieldDelimiter": ",",
                "encoding": "UTF-8",
                "autodetect": True,  # 스키마 자동 감지 추가
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    execute_sql_query =  BigQueryExecuteQueryOperator(
        task_id='execute_sql_query',
        sql="""
        -- 2019부터 2023년까지 사고 건수 쿼리 결과를 새로운 테이블에 삽입
        CREATE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.bigquery_dataset_id }}.{{ var.value.bigquery_dataset_id_2 }} AS 
        WITH total_accidents AS (
            SELECT 
                PRD_DE, 
                C2_NM, 
                SUM(DT) AS total_accidents
            FROM 
                `{{ var.value.gcp_project_id }}.{{ var.value.bigquery_dataset_id }}.kosis_data_defore`
            WHERE 
                PRD_DE BETWEEN 2019 AND 2023
                AND C2_NM = '전체'
                AND ITM_NM = '사고건수'
            GROUP BY 
                PRD_DE, C2_NM
        ),
        elderly_accidents AS (
            SELECT 
                PRD_DE, 
                C2_NM, 
                SUM(DT) AS total_accidents
            FROM 
                `{{ var.value.gcp_project_id }}.{{ var.value.bigquery_dataset_id }}.kosis_data_defore`
            WHERE 
                PRD_DE BETWEEN 2019 AND 2023
                AND C2_NM = '전체'
                AND C1_NM IN ('65-70세', '71세이상')
                AND ITM_NM = '사고건수'
            GROUP BY 
                PRD_DE, C2_NM
        )
        SELECT 
            all_accidents.PRD_DE AS year,
            all_accidents.C2_NM AS month,
            all_accidents.total_accidents AS total_accidents,  -- 전체 사고 건수
            elderly_accidents.total_accidents AS elderly_accidents, -- 고령자 사고 건수
            ROUND(
                (elderly_accidents.total_accidents / all_accidents.total_accidents) * 100, 
                2
            ) AS elderly_accidents_percentage -- 고령자 사고 비율 (반올림)
        FROM 
            total_accidents all_accidents
        LEFT JOIN 
            elderly_accidents
        ON 
            all_accidents.PRD_DE = elderly_accidents.PRD_DE
            AND all_accidents.C2_NM = elderly_accidents.C2_NM
        ORDER BY 
            year, month; -- 연도와 월 기준 정렬
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',  # 구글 클라우드 연결 ID
    )

    # 태스크 순서 정의
    fetch_json_task >> json_to_csv_task >> gcs_to_bigquery_task >> execute_sql_query