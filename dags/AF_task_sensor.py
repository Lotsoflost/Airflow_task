import pandas as pd
from airflow import DAG
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance
from airflow.providers.http.operators.http import HttpOperator
from airflow.datasets import Dataset
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient

from airflow.sdk import TaskGroup
from pandas.errors import EmptyDataError



def response_operator(response, **kwargs):
    """
    Validates HTTP response and saves the downloaded file locally.
    Returns True only if the response is successful and not an HTML page.
    """
    if response.status_code == 200 and 'text/html' not in response.headers.get('Content-Type', ''):
        with open("/tmp/report", "wb") as fp:
            fp.write(response.content)
        return True
    return False


def branch_func(**kwargs):
    """
    Branches execution depending on whether the ZIP file
    contains data or is empty.
    """
    try:
        df = pd.read_csv("/tmp/report", compression='zip')
    except EmptyDataError:
        return 'function_if_empty'
    else:
        return 'function_if_full'


def transform_data_null(raw_file, csv_file):
    """
    Replaces null values and string 'null' with a dash.
    """
    df = pd.read_csv(raw_file, compression='zip')
    df = df.fillna("-").replace("null", "-")
    df.to_csv(csv_file, encoding="utf-8", index=False)


def transform_data_sort(raw_file, csv_file):
    """
    Sorts data by created_date.
    """
    df = pd.read_csv(raw_file)
    df = df.sort_values("created_date")
    df.to_csv(csv_file, encoding="utf-8", index=False)


def transform_data_emoji(raw_file, csv_file):
    """
    Removes emojis and unwanted characters from string columns,
    leaving only text and punctuation.
    """
    df = pd.read_csv(raw_file)
    for col in df.columns:
        if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^\w\s\.,!?;:'\"()\-\[\]]+", "", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )
    df.to_csv(csv_file, encoding="utf-8", index=False)


# Dataset used to trigger downstream DAG when final file is updated
mongo_ready_data = Dataset("file:///tmp/transformed_final.csv")

with DAG(
        dag_id='00_mongo_start',
        schedule=timedelta(hours=1),
        start_date=datetime(2026, 1, 1),
        tags={'AF', 'pipeline'},
        catchup=False,
) as dag:

    check_report_task = HttpSensor(
        task_id='check_report_task',
        http_conn_id='gdrive_filepath',
        endpoint="",
        response_check=response_operator,
        method='GET',
        poke_interval=2,
        timeout=60,
        mode='poke',
    )

    # Executed if the file is empty
    function_if_empty = BashOperator(
        task_id='function_if_empty',
        bash_command="echo 'File is empty'",
    )

    # Executed if the file contains data
    function_if_full = EmptyOperator(
        task_id='function_if_full',
    )

    with TaskGroup("transform_tasks", dag=dag) as transform_group:
        task_null = PythonOperator(
            task_id='transform_null',
            python_callable=transform_data_null,
            op_args=['/tmp/report', '/tmp/report_null.csv'],
        )

        task_sort = PythonOperator(
            task_id='transform_sort',
            python_callable=transform_data_sort,
            op_args=['/tmp/report_null.csv', '/tmp/report_sorted.csv'],
        )

        task_emoji = PythonOperator(
            task_id='transform_emoji',
            python_callable=transform_data_emoji,
            op_args=['/tmp/report_sorted.csv', '/tmp/transformed_final.csv'],
            outlets=[mongo_ready_data]  # Triggers downstream DAG via Dataset
        )

        task_null >> task_sort >> task_emoji
        function_if_full >> transform_group

    # Branching based on file content
    branch_op = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
    )

    check_report_task >> branch_op >> [function_if_empty, function_if_full]
