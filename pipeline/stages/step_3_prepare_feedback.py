import logging

from airflow import DAG

from utils.workspace import inside_workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace
def prepare_feedback(ds, ts, dag: DAG, **kwargs):
    """
    Prepare feedback as DataFrame with unified schema
    :param ds:
    :param kwargs:
    :return:
    """
    pass


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    prepare_feedback(None, current_ts, MockDag)
