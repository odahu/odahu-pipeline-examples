from datetime import datetime

from airflow import DAG
from odahuflow.airflow_plugin.deployment import DeploymentOperator, DeploymentSensor
from odahuflow.airflow_plugin.model import ModelPredictRequestOperator, ModelInfoRequestOperator
from odahuflow.airflow_plugin.packaging import PackagingOperator, PackagingSensor
from odahuflow.airflow_plugin.resources import resource
from odahuflow.airflow_plugin.training import TrainingOperator, TrainingSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'end_date': datetime(2099, 12, 31)
}

api_connection_id = "odahuflow_api"
model_connection_id = "odahuflow_model"

training_id, training = resource('manifests/training.odahuflow.yaml')

packaging_id, packaging = resource('manifests/packaging.odahuflow.yaml')

deployment_id, deployment = resource('manifests/deployment.odahuflow.yaml')

model_example_request = {
    "columns": [
        "text"
    ],
    "data": [[
        "The Bank of England said ita had provided the\n    money market with a further 437 mln stg assistance in the\n    afternoon session. This brings the Bank's total help so far\n    today to 461 mln stg and compares with its revised shortage\n    forecast of 450 mln stg.\n        The central bank made purchases of bank bills outright\n    comprising 120 mln stg in band one at 10-7\/8 pct and 315 mln\n    stg in band two at 10-13\/16 pct.\n        In addition, it also bought two mln stg of treasury bills\n    in band two at 10-13\/16 pct.\n    "
    ]]
}

dag = DAG(
    'airflow-reuters',
    default_args=default_args,
    schedule_interval=None
)

with dag:
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

    model_predict_request = ModelPredictRequestOperator(
        task_id="model_predict_request",
        model_deployment_name=deployment_id,
        api_connection_id=api_connection_id,
        model_connection_id=model_connection_id,
        request_body=model_example_request,
        default_args=default_args
    )

    model_info_request = ModelInfoRequestOperator(
        task_id='model_info_request',
        model_deployment_name=deployment_id,
        api_connection_id=api_connection_id,
        model_connection_id=model_connection_id,
        default_args=default_args
    )

    train >> wait_for_train >> pack >> wait_for_pack >> dep >> wait_for_dep
    wait_for_dep >> model_info_request
    wait_for_dep >> model_predict_request
