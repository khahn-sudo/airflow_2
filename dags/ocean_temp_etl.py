from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# MySQL에서 데이터 읽는 함수
def read_ocean_table():
    # MySQL Hook을 사용하여 연결
    mysql_hook = MySqlHook(mysql_conn_id='mysql_ocean_connection')  # 연결 ID는 설정한 연결 이름
    query = "SELECT * FROM ocean_admin.ocean"  # 쿼리 작성
    
    # 쿼리 실행 및 결과 가져오기
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    
    # 결과 출력 (로깅)
    result = cursor.fetchall()
    for row in result:
        print(row)  # 혹은 Airflow의 로그로 출력됨

    cursor.close()

# DAG 정의
with DAG(
    'mysql_ocean_read',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 1, 7),
        'retries': 1,
    },
    schedule_interval=None,  # 수동으로 실행할 때는 None으로 설정
    catchup=False,
) as dag:

    # PythonOperator로 데이터 읽기 작업 실행
    read_data_task = PythonOperator(
        task_id='read_ocean_data',
        python_callable=read_ocean_table,
    )

    read_data_task  # 실행
