import os
import json
import random
from datetime import datetime
from airflow.decorators import dag, task


@task
def install_and_import():
    import subprocess
    subprocess.check_call(["pip", "install", "requests"])
    subprocess.check_call(["pip", "install", "numpy"])
    subprocess.check_call(["pip", "install", "pandas"])
    subprocess.check_call(["pip", "install", "sklearn"])
    # subprocess.check_call(["pip", "install", "matplotlib"])
    # subprocess.check_call(["pip", "install", "seaborn"])
    subprocess.check_call(["pip", "install", "minio"])
    return
    
    
@task
def get_twitter_data():
    import pandas as pd
    data = [[random.randint(0,10),random.randint(50,150),random.randint(1,110),random.randint(25,75),] for _ in range(100)]
    df = pd.DataFrame(data, columns=["a", "b", "c", "d"])
    return df


@task
def dump_data_to_bucket(tweet_list: list):
    import pandas as pd
    from minio import Minio
    from io import BytesIO

    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    df = pd.DataFrame(tweet_list)
    csv = df.to_csv(index=False).encode("utf-8")

    client = Minio("http://s3.object.com", access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

    # Make MINIO_BUCKET_NAME if not exist.
    found = client.bucket_exists(MINIO_BUCKET_NAME)
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


twitter_etl()
