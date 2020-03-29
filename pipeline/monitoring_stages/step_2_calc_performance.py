import json
import logging
from typing import Dict

from airflow import DAG

from utils.workspace import inside_workspace
from utils.misc import harvest_json_in_tree

logger = logging.getLogger(__name__)


def wrong_feedback_share() -> float:
    """
    Show share of received feedback in count of all model requests
    This concept suggests that system get feedback only on wrong predictions
    :return:
    """

    total_inferences = 0
    total_errors = 0

    requests = harvest_json_in_tree('request_response')
    for request in requests:
        with open(request, 'rt') as f:
            req: Dict = json.load(f)

        req_content = json.loads(req['request_content'])
        # because system can make N inferences for one HTTP request we need to sum all inferences
        total_inferences += len(req_content['data'])

    feedback = harvest_json_in_tree('feedback')
    for error_file in feedback:
        with open(error_file, 'rt') as f:
            err: Dict = json.load(f)

        # because system can set feedback for N inferences for one request sum all errors
        total_errors += len(err['payload']['json']['data'])

    error_share = round(total_errors / total_inferences, 2)
    return error_share


@inside_workspace()
def calc_performance(ds, ts, dag: DAG, **kwargs):
    """
    Analyze feedback and measure system performance metrics
    :param ds:
    :param kwargs:
    :return:
    """
    share = wrong_feedback_share()
    logger.info(f'Model performance: errors share for all inferences: {share}')
    with open('metrics.json', 'wt') as f:
        json.dump({'wrong share': share}, f)


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag_mon'

    current_ts = 'local_ts'
    calc_performance(None, current_ts, MockDag)
