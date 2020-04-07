import logging
import pickle

from airflow import DAG
import pandas as pd
import numpy as np

from utils import const
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

    df: pd.DataFrame = pd.read_parquet(const.COMBINED_DATASET_PARQUET_FILE)

    # validate and fix dataframe (remove empty where text is empty)
    df['text'].replace('', np.nan, inplace=True)
    df.dropna(subset=['text'], inplace=True)

    df.to_parquet(const.COMBINED_DATASET_PARQUET_FILE)


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    validate_input_dataframe(None, current_ts, MockDag)
