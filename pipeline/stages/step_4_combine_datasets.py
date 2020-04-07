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
    base_df: pd.DataFrame = pd.read_parquet(const.DATASET_PARQUET_FILE)
    feedback_df: pd.DataFrame = pd.read_parquet(const.FEEDBACK_DATASET_PARQUET_FILE)

    combined_df: pd.DataFrame = pd.concat([base_df, feedback_df], ignore_index=True, sort=False)

    combined_df.to_parquet(const.COMBINED_DATASET_PARQUET_FILE)
    logger.info(f'DataFrame with feedback is saved in {const.COMBINED_DATASET_PARQUET_FILE}')


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    combine_datasets(None, current_ts, MockDag)
