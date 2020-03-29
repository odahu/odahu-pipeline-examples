import logging
import pickle

from airflow import DAG
import pandas as pd

from utils import const
from utils.workspace import inside_workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace()
def combine_datasets(ds, ts, dag: DAG, **kwargs):
    """
    Combine feedback and base data sets
    :param ds:
    :param kwargs:
    :return:
    """
    with open(const.DATASET_PICKLE_FILE, 'rb') as f:
        base_df: pd.DataFrame = pickle.load(f)

    with open(const.FEEDBACK_DATASET_PICKLE_FILE, 'rb') as f:
        feedback_df: pd.DataFrame = pickle.load(f)

    combined_df = pd.concat([base_df, feedback_df], ignore_index=True, sort=False)

    with open(const.COMBINED_DATASET_PICKLE_FILE, 'wb') as f:
        pickle.dump(combined_df, f)
        logger.info(f'DataFrame with feedback is saved in {const.COMBINED_DATASET_PICKLE_FILE}')


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    combine_datasets(None, current_ts, MockDag)
