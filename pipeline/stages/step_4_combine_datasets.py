import logging

from airflow import DAG

from utils.workspace import inside_workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace
def combine_datasets(ds, ts, dag: DAG, **kwargs):
    """
    Combine feedback and base data sets
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
    combine_datasets(None, current_ts, MockDag)
