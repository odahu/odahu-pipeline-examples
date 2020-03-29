import json
import logging
import pickle
from typing import List, Dict

import requests
from airflow import DAG
import pandas as pd

from utils import const
from utils.workspace import inside_workspace
from utils import odahu_api

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def interpret(request_result):

    topics_list: List[List[str]] = []

    predictions, columns = request_result['prediction'], request_result['columns']

    for pred in predictions:  # type: List[str]
        topics = []
        for i, topic_prob in enumerate(pred):
            if float(topic_prob.replace('%', '')) > 0.30:
                topics.append(columns[i])
        topics_list.append(topics)
    return topics_list


@inside_workspace()
def feedback(ds, ts, dag: DAG, **kwargs):
    """
    Send feedback to ODAHU
    :param ds:
    :param kwargs:
    :return:
    """

    with open(const.PREPARED_FEEDBACK, 'rt') as f:
        obj = json.load(f)
        headers, data = obj['headers'], obj['feedback']
        odahu_api.feedback(headers, data)


if __name__ == '__main__':

    class MockDag:
        dag_id = 'local_dag_inference'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    feedback(None, current_ts, MockDag)
