import json
import logging
import pickle

import requests
from airflow import DAG
import pandas as pd

from utils import const
from utils.workspace import inside_workspace
from utils import odahu_api

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace()
def make_inference(ds, ts, dag: DAG, **kwargs):
    """
    Make inference for realtime data
    :param ds:
    :param kwargs:
    :return:
    """
    with open(const.REALDATA_PICKLE, 'rb') as f:
        docs_df: pd.DataFrame = pickle.load(f)

    sample: pd.DataFrame = docs_df.sample(10)
    request_body = sample.drop(const.TOPICS_COL, 1)
    request_result, headers = odahu_api.predict(request_body.to_json(orient='split'))
    with open(const.INFERENCE_RESULT, 'wt') as f:
        json.dump(request_result, f)
    with open(const.REAL_RESULT, 'wt') as f:
        f.write(sample.to_json(orient='split'))
    with open(const.INFERENCE_HEADERS, 'wt') as f:
        json.dump(dict(headers), f)


if __name__ == '__main__':

    class MockDag:
        dag_id = 'local_dag_inference'

    current_ts = 'local_ts'
    make_inference(None, current_ts, MockDag)
