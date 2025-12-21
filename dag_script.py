from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Etraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading 

defautl_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),    
    'email': 'akingboyekunle@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG(
    'ETL_Pipeline_ZipcoFood',
    default_args=defautl_args,
    description='An ETL pipeline for ZipcoFood data using Airflow',
    catchup=False # do not backfill missed runs
)

extraction_task = PythonOperator(
    task_id='extraction_layer',
    python_callable=run_extraction,
    dag=dag
)

transformation_task = PythonOperator(
    task_id='transformation_layer', 
    python_callable=run_transformation,
    dag=dag
)

loading_task = PythonOperator(
    task_id='loading_layer',    
    python_callable=run_loading,
    dag=dag
)

extraction_task >> transformation_task >> loading_task