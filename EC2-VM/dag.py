from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from emr_process import remove_s3_path,etl_process,analyze_and_store


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 7),
    'email': ['jenlyashik@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'log_analysis_DAG',
    default_args=default_args,
    description='DAF for end to end ETL and anaysis',
    schedule=timedelta(weeks=1),
)

remove_s3_path =  PythonOperator(
    task_id = "Remove_s3_path",
    python_callable=remove_s3_path,
    op_args=["Remove s3 path","aws s3 -rm <path> --recursive"],
    dag=dag
)


etl_process = PythonOperator(
    task_id = "ETL_process",
    python_callable=etl_process,
    op_args=["ETL process","spark-submit ~/program/etl.py"],
    dag=dag
)

analyze_and_store = PythonOperator(
    task_id = "Analyze_and_Storing",
    python_callable=analyze_and_store,
    op_args=["Analyze and Storing","spark-submit ~/program/analyze.py"],
    dag=dag
)


remove_s3_path >> etl_process >> analyze_and_store