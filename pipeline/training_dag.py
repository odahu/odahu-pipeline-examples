"""
This is example of inference pipeline

1. Extract data
    1.1 Extract initial data from base dataset
    1.2 Extract feedback data about previous version model predictions

2. Prepare data:
    2.1 Transform data to proper unified format
        2.1.1  Transform data from base dataset
        2.1.2  Transform data from feedback loop
    2.2 Validate data
    2.3 Fit tokenizer (and save it as pipeline artifact)
    2.4 Split on train, validation, test sets (upload test set as pipeline artifact)
3. Train (using ODAHU)
4. Evaluate performance using test dataset
5. Package (using ODAHU)
6. Deploy (using ODAHU)
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from odahuflow.airflow_plugin.deployment import DeploymentOperator, DeploymentSensor
from odahuflow.airflow_plugin.packaging import PackagingOperator, PackagingSensor
from odahuflow.airflow_plugin.resources import resource
from odahuflow.airflow_plugin.training import TrainingOperator, TrainingSensor

from stages import prepare_base_dataset, prepare_odahu_training

api_connection_id = "odahuflow_api"
model_connection_id = "odahuflow_model"
training_id, training = resource('manifests/training.odahuflow.yaml')
packaging_id, packaging = resource('manifests/packaging.odahuflow.yaml')
deployment_id, deployment = resource('manifests/deployment.odahuflow.yaml')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'end_date': datetime(2099, 12, 31)
}

dag = DAG(
    'reuters-training',
    default_args=default_args,
    schedule_interval=None
)

with dag:

    prepare_base_dataset_task = PythonOperator(
        python_callable=prepare_base_dataset,
        task_id='prepare_base_dataset',
        provide_context=True,
        default_args=default_args
    )

    prepare_odahu_training_task = PythonOperator(
        python_callable=prepare_odahu_training,
        task_id='prepare_input_training',
        provide_context=True,
        default_args=default_args
    )

    train = TrainingOperator(
        task_id="training",
        api_connection_id=api_connection_id,
        training=training,
        default_args=default_args
    )

    wait_for_train = TrainingSensor(
        task_id='wait_for_training',
        training_id=training_id,
        api_connection_id=api_connection_id,
        default_args=default_args
    )

    pack = PackagingOperator(
        task_id="packaging",
        api_connection_id=api_connection_id,
        packaging=packaging,
        trained_task_id="wait_for_training",
        default_args=default_args
    )

    wait_for_pack = PackagingSensor(
        task_id='wait_for_packaging',
        packaging_id=packaging_id,
        api_connection_id=api_connection_id,
        default_args=default_args
    )

    dep = DeploymentOperator(
        task_id="deployment",
        api_connection_id=api_connection_id,
        deployment=deployment,
        packaging_task_id="wait_for_packaging",
        default_args=default_args
    )

    wait_for_dep = DeploymentSensor(
        task_id='wait_for_deployment',
        deployment_id=deployment_id,
        api_connection_id=api_connection_id,
        default_args=default_args
    )

    # PIPELINE

    prepare_base_dataset_task >> prepare_odahu_training_task >> train >> wait_for_train

    wait_for_train >> pack >> wait_for_pack

    wait_for_pack >> dep >> wait_for_dep
