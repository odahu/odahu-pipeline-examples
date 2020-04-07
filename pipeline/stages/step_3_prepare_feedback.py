import json
import logging
import pickle
from typing import Dict

from airflow import DAG
import pandas as pd

from utils import const
from utils.misc import harvest_json_in_tree
from utils.workspace import inside_workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace()
def prepare_feedback(ds, ts, dag: DAG, **kwargs):
    """
    Prepare feedback as DataFrame with unified schema
    :param ds:
    :param kwargs:
    :return:
    """
    feedback = harvest_json_in_tree('model_log/feedback')

    all_feedback = []
    for error_file in feedback:
        with open(error_file, 'rt') as f:
            err: Dict = json.load(f)

        all_feedback += err['payload']['json']['data']

    df = pd.DataFrame(columns=['text', 'topics'], data=all_feedback)

    df.to_parquet(const.FEEDBACK_DATASET_PARQUET_FILE)
    logger.info(f'DataFrame with feedback is saved in {const.FEEDBACK_DATASET_PARQUET_FILE}')


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    prepare_feedback(None, current_ts, MockDag)
