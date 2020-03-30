"""
This is example of inference dag

1. Make inference using ODAHU deployed model
2. Interpret data
3. Compare with real value
4. Send feedback about prediction using ODAHU API
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from inference_stages import extract_realtime_data, make_inference, interpret_data, feedback
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

    interpret_data_task = PythonOperator(
        python_callable=interpret_data,
        task_id='interpret_data',
        provide_context=True,
        default_args=default_args
    )

    feedback_task = PythonOperator(
        python_callable=feedback,
        task_id='feedback',
        provide_context=True,
        default_args=default_args
    )

    interpret_data_task >> feedback_task
