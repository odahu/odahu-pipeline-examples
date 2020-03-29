"""
This is example of training (and re-training) dag
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from odahuflow.airflow_plugin.deployment import DeploymentOperator, DeploymentSensor
from odahuflow.airflow_plugin.packaging import PackagingOperator, PackagingSensor
from odahuflow.airflow_plugin.resources import resource
from odahuflow.airflow_plugin.training import TrainingOperator, TrainingSensor
from odahuflow.sdk.models import ModelTraining, ModelPackaging, ModelDeployment

from stages import prepare_base_dataset, prepare_odahu_training, extract_feedback, \
    prepare_feedback, combine_datasets, validate_input_dataframe
from __version__ import VERSIONED_PROJECT, VERSION, PROJECT

api_connection_id = "odahuflow_api"
model_connection_id = "odahuflow_model"

# Load ODAHU API manifests and change versions according pipeline version

_, training = resource('manifests/training.odahuflow.yaml')   # type: str, ModelTraining
_, packaging = resource('manifests/packaging.odahuflow.yaml')  # type: str, ModelPackaging
_, deployment = resource('manifests/deployment.odahuflow.yaml')  # type: str, ModelDeployment

training_id, packaging_id, deployment_id = VERSIONED_PROJECT, VERSIONED_PROJECT, VERSIONED_PROJECT

training.id = VERSIONED_PROJECT
packaging.id = VERSIONED_PROJECT
deployment.id = VERSIONED_PROJECT

training.spec.model.name = PROJECT
training.spec.model.version = VERSION

# Dag section

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'end_date': datetime(2099, 12, 31)
}

dag = DAG(
    f'{VERSIONED_PROJECT}-training',
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

    extract_feedback_task = PythonOperator(
        python_callable=extract_feedback,
        task_id='extract_feedback',
        provide_context=True,
        default_args=default_args
    )

    prepare_feedback_task = PythonOperator(
        python_callable=prepare_feedback,
        task_id='prepare_feedback',
        provide_context=True,
        default_args=default_args
    )

    combine_datasets_task = PythonOperator(
        python_callable=combine_datasets,
        task_id='combine_datasets',
        provide_context=True,
        default_args=default_args
    )

    validate_input_dataframe_task = PythonOperator(
        python_callable=validate_input_dataframe,
        task_id='validate_input_dataframe',
        provide_context=True,
        default_args=default_args
    )

    prepare_odahu_training_task = PythonOperator(
        python_callable=prepare_odahu_training,
        task_id='prepare_odahu_training_task',
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

    extract_feedback_task >> prepare_feedback_task >> combine_datasets_task

    prepare_base_dataset_task >> combine_datasets_task

    combine_datasets_task >> validate_input_dataframe_task

    validate_input_dataframe_task >> prepare_odahu_training_task >> train >> wait_for_train

    wait_for_train >> pack >> wait_for_pack

    wait_for_pack >> dep >> wait_for_dep
