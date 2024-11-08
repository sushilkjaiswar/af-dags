from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from random import randint
import pandas as pd
import logging

def install_and_import():
    import subprocess
    subprocess.check_call(["pip", "install", "pandas"])

def process_data():
    logging.info("start data process..")
    df = pd.DataFrame([[21,32,12],[1,4,2],[45,89,65]], columns=['a', 'b', 'c'])
    df.head()

with DAG("dynamic_installation", start_date=datetime(2024, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    run_task = PythonOperator(
        task_id="install_library",
        python_callable=install_and_import
    )

    process_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data
    )
    run_task >> process_task
