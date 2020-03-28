"""
This is example of inference pipeline

1. Make inference using ODAHU deployed model
2. Send feedback about prediction using ODAHU API
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from stages import prepare_base_dataset, prepare_odahu_training

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'end_date': datetime(2099, 12, 31)
}

dag = DAG(
    'reuters-inference',
    default_args=default_args,
    schedule_interval=None
)

with dag:

    extract_data_op = PythonOperator(
        python_callable=prepare_base_dataset,
        task_id='prepare_base_dataset',
        provide_context=True,
        default_args=default_args
    )

    prepare_training = PythonOperator(
        python_callable=prepare_odahu_training,
        task_id='prepare_input_training',
        provide_context=True,
        default_args=default_args
    )

    extract_data_op >> prepare_training

