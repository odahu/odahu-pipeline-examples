import json
import logging

from airflow import DAG

from utils.workspace import inside_workspace
from utils import config

logger = logging.getLogger(__name__)

ERROR_RATE_LIMIT = 0.10


@inside_workspace()
def get_error_rate(ds, ts, dag: DAG, **kwargs):
    with open('metrics.json', 'rt') as f:
        metrics = json.load(f)
    return metrics['wrong share']


def trigger_retraining(context, obj):
    """
    Extract feedback to workspace
    :param ds:
    :param kwargs:
    :return:
    """
    error_rate = get_error_rate(context['ds'], context['ts'], context['dag'])
    if error_rate > ERROR_RATE_LIMIT:
        logger.warning(f'Attention: model error rate={error_rate} > {ERROR_RATE_LIMIT}. Run re-training model')
        return obj


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag_mon'

    context = {
        'dag': MockDag,
        'ts': 'local_ts',
        'ds': None
    }
    current_ts = 'local_ts'
    trigger_retraining(context, object())
