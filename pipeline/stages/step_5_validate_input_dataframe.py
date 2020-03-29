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
    with open(const.COMBINED_DATASET_PICKLE_FILE, 'rb') as f:
        df: pd.DataFrame = pickle.load(f)

    # validate and fix dataframe (remove empty where text is empty)
    df['text'].replace('', np.nan, inplace=True)
    df.dropna(subset=['text'], inplace=True)

    with open(const.COMBINED_DATASET_PICKLE_FILE, 'wb') as f:
        pickle.dump(df, f)


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    validate_input_dataframe(None, current_ts, MockDag)
