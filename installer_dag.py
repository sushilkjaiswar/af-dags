from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from random import randint

def install_and_import():
    import subprocess
    subprocess.check_call(["pip", "install", "pandas"])

with DAG("dynamic_installation", start_date=datetime(2024, 1, 1)) as dag:
    run_task = PythonOperator(
        task_id="install_library",
        python_callable=install_and_import
    )
