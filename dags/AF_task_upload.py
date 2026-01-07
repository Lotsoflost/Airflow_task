import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

from airflow.sdk.definitions.asset import Dataset
from pymongo import MongoClient

mongo_ready_data = Dataset("file:///tmp/transformed_final.csv")

def upload_to_mongo(csv_file):
    # Чтение данных из CSV

    client = MongoClient('mongodb://host.docker.internal:27017/')
    db = client['local']  # Имя базы данных
    collection = db['airflow_data']  # Имя коллекции

    df = pd.read_csv(csv_file)
    # 3. Преобразуем DataFrame в список словарей
    data = df.to_dict(orient='records')

    # 4. Загружаем в монгу
    result = collection.insert_many(data)

    print(f"Documents uploaded: {len(result.inserted_ids)}")

with DAG(
    dag_id='01_mongo_upload',
    schedule=[mongo_ready_data], # This DAG starts as soon as the file is ready
    start_date=datetime(2025, 1, 1),
    # ...
) as dag2:
    task_upload = PythonOperator(
            task_id='upload_to_mongo',
            python_callable=upload_to_mongo,
            op_args=['/tmp/transformed_final.csv'],
        )