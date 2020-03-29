import logging
import os

from airflow import DAG

from utils.workspace import inside_workspace
from utils import config, const
from utils.misc import gcs_sync

logger = logging.getLogger(__name__)


@inside_workspace()
def extract_feedback(ds, ts, dag: DAG, **kwargs):
    """
    Extract feedback to workspace
    :param ds:
    :param kwargs:
    :return:
    """
    gcs_sync(f'{const.GS_FEEDBACK_PATH}/*', './')


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag_mon'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    extract_feedback(None, current_ts, MockDag)
