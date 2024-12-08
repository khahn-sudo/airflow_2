from airflow import DAG
import datetime
import pendulum
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 12, 7, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["session2", "init_test"],
    # params={"example_key": "example_value"},
) as dag:
    test_1 = EmptyOperator(
        task_id="test_1",
        bash_command="echo test 1"
    ),

    test_2 = EmptyOperator(
        task_id="test_2",
        bash_command="echo $HOSTNAME"
    )

    test_1 >> test_2