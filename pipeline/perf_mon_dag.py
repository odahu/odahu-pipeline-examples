"""
This is example model monitoring DAG

1. Extract feedback
2. Check performance
3. Run re-training


"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from monitoring_stages import extract_feedback, calc_performance, trigger_retraining
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
    f'{VERSIONED_PROJECT}-monitoring',
    default_args=default_args,
    schedule_interval=None
)

with dag:

    extract_feedback_task = PythonOperator(
        python_callable=extract_feedback,
        task_id='extract_feedback',
        provide_context=True,
        default_args=default_args
    )

    calc_performance_task = PythonOperator(
        python_callable=calc_performance,
        task_id='calc_performance',
        provide_context=True,
        default_args=default_args
    )

    trigger_retraining_task = TriggerDagRunOperator(
        trigger_dag_id=f'{VERSIONED_PROJECT}-training',
        python_callable=trigger_retraining,
        task_id='trigger_retraining',
        provide_context=True,
        default_args=default_args
    )

    extract_feedback_task >> calc_performance_task >> trigger_retraining_task


