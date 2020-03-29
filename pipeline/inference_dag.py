"""
This is example of inference pipeline

1. Make inference using ODAHU deployed model
2. Send feedback about prediction using ODAHU API
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from inference_stages import extract_realtime_data, make_inference
from __version__ import VERSIONED_PROJECT

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'end_date': datetime(2099, 12, 31)
}

dag = DAG(
    f'{VERSIONED_PROJECT}-inference',
    default_args=default_args,
    schedule_interval=None
)

with dag:

    extract_data_task = PythonOperator(
        python_callable=extract_realtime_data,
        task_id='extract_realtime_data',
        provide_context=True,
        default_args=default_args
    )

    make_inference_task = PythonOperator(
        python_callable=make_inference,
        task_id='make_inference',
        provide_context=True,
        default_args=default_args
    )

    extract_data_task >> make_inference_task

