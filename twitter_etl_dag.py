import os
import json
import random
from datetime import datetime
from airflow.decorators import dag, task
import logging

@task
def install_and_import():
    import subprocess
    
    subprocess.check_call(["pip", "install", "numpy"])
    subprocess.check_call(["pip", "install", "pandas"])
    subprocess.check_call(["pip", "install", "scikit-learn"])
    # subprocess.check_call(["pip", "install", "matplotlib"])
    # subprocess.check_call(["pip", "install", "seaborn"])
    subprocess.check_call(["pip", "install", "minio"])
    return []
    
    
@task
def get_twitter_data(data):
    import pandas as pd
    data = [[random.randint(0,10),random.randint(50,150),random.randint(1,110),random.randint(25,75),] for _ in range(100)]

    return data


@task
def dump_data_to_bucket(tweet_list: list):
    import pandas as pd
    from minio import Minio
    from io import BytesIO

    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    df = pd.DataFrame(tweet_list, columns=["a", "b", "c", "d"])
    csv = df.to_csv(index=False).encode("utf-8")
    logging.info(f"MINIO_ROOT_USER: {MINIO_ROOT_USER}, MINIO_BUCKET_NAME: {MINIO_BUCKET_NAME}")
    client = Minio("http://s3.minio.com", access_key="admin", secret_key="adminadmin", secure=False)

    # Make MINIO_BUCKET_NAME if not exist.
    found = client.bucket_exists("data-warehouse")
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
    else:
        print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

    # Put csv data in the bucket
    client.put_object(
        "airflow-bucket", "twitter_elon_musk.csv", data=BytesIO(csv), length=len(csv), content_type="application/csv"
    )


@dag(
    schedule="0 */2 * * *",
    start_date=datetime(2022, 12, 26),
    catchup=False,
    tags=["twitter", "etl"],
)
def twitter_etl():
    dump_data_to_bucket(get_twitter_data(install_and_import()))
    # get_twitter_data(install_and_import())


twitter_etl()
