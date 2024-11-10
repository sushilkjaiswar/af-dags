from airflow import DAG
from airflow.datasets import Dataset
from tempfile import NamedTemporaryFile
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from random import randint
from minio import Minio
import pandas as pd
import pendulum
import logging
import os 

wildfires_dataset = Dataset("s3://s3.minio.com/data-warehouse/ml/")
os.environ["HOURS_AGO"] = "0"

def _transfer_from_api_to_s3():
    logging.info(os.environ)
    data = [{"id":1,"first_name":"Rosco","last_name":"Reddyhoff","email":"rreddyhoff0@blogger.com","gender":"Male","ip_address":"9.46.93.93"},
            {"id":2,"first_name":"Kaye","last_name":"Barbary","email":"kbarbary1@seesaa.net","gender":"Female","ip_address":"57.90.84.94"},
            {"id":3,"first_name":"Hamlen","last_name":"Grigs","email":"hgrigs2@newyorker.com","gender":"Male","ip_address":"10.222.119.144"},
            {"id":4,"first_name":"Gus","last_name":"Shaw","email":"gshaw3@oracle.com","gender":"Female","ip_address":"106.105.77.55"},
            {"id":5,"first_name":"Roxanne","last_name":"Knappett","email":"rknappett4@symantec.com","gender":"Female","ip_address":"76.50.246.78"},
            {"id":6,"first_name":"Joceline","last_name":"Sorrill","email":"jsorrill5@prlog.org","gender":"Female","ip_address":"86.119.91.128"},
            {"id":7,"first_name":"Bengt","last_name":"Chestnut","email":"bchestnut6@baidu.com","gender":"Male","ip_address":"24.4.76.48"},
            {"id":8,"first_name":"Jennee","last_name":"Regus","email":"jregus7@hugedomains.com","gender":"Female","ip_address":"129.62.122.124"},
            {"id":9,"first_name":"Monte","last_name":"McCotter","email":"mmccotter8@bloglines.com","gender":"Male","ip_address":"199.222.250.48"},
            {"id":10,"first_name":"Shaylyn","last_name":"Fonte","email":"sfonte9@google.it","gender":"Female","ip_address":"234.1.220.34"}]
  
    df = pd.DataFrame.from_records(data)
    filename = "user.csv"
    bucket = "data-warehouse"
    key = "ml"
    with NamedTemporaryFile("w+b") as file:
        df.to_csv(filename, index=False)

        minio_client = client = Minio("minio-service.s3-system:9000", access_key="admin", secret_key="adminadmin", secure=False)

        logging.info("Storing object: %s/%s.", bucket, key)

        minio_client.remove_object(bucket_name=bucket, object_name=key)

        result = minio_client.fput_object(
            bucket_name=bucket, object_name=key, file_path=filename
        )

        logging.info(result)


def _list_objects():
    filename = "user.csv"
    bucket = "data-warehouse"
    key = "ml/user.csv"
  
    minio_client = Minio("minio-service.s3-system:9000", access_key="admin", secret_key="adminadmin", secure=False)

    result = minio_client.list_objects(bucket_name=bucket)

    logging.info("Listing objects:")
    logging.info(f"Result: {result}")
    for item in result:
        logging.info(item.object_name)


with DAG(
    dag_id="data_aware_producer_dataset_with_trigger",
    description="This dag demonstrates a simple dataset producer",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    tags=["dataset-producer", "empty-operator"],
):


    transfer_from_api_to_s3 = PythonOperator(
        task_id="transfer_api_to_s3",
        python_callable=_transfer_from_api_to_s3,
    )

    trigger = EmptyOperator(task_id="triggerer", outlets=[wildfires_dataset])

    transfer_from_api_to_s3 >> trigger


with DAG(
    dag_id="data_aware_consumer_dataset_with_trigger",
    description="This dag demonstrates a simple dataset consumer",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule=[wildfires_dataset],
    tags=["dataset-consumer", "empty-operator"],
    catchup=False,
):
    list_objects = PythonOperator(
        task_id="list_objects",
        python_callable=_list_objects,
    )
