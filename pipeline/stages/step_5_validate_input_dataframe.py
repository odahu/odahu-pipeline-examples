import logging

from airflow import DAG

from utils.workspace import inside_workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace()
def validate_input_dataframe(ds, ts, dag: DAG, **kwargs):
    """
    Validate input dataframe
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
    validate_input_dataframe(None, current_ts, MockDag)
