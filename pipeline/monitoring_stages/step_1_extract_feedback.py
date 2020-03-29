import logging
import os
import pickle

import pandas as pd
from airflow import DAG

from utils import const
from utils.data_extractor import download_tar_gz
from utils.parser import stream_reuters_body_topics
from contextlib import suppress

from utils.workspace import inside_workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace()
def extract_realtime_data(ds, ts, dag: DAG, **kwargs):
    """
    Extract realtime data for inference
    :param ds:
    :param kwargs:
    :return:
    """
    folder = 'realtime_dataset'
    with suppress(FileExistsError):
        os.mkdir(folder)
    download_tar_gz(f'data/realtime_dataset.tar.gz', folder)
    docs = list(stream_reuters_body_topics(folder))
    docs_df = pd.DataFrame(data=docs, columns=['text', 'topics'])

    with open(const.REALDATA_PICKLE, 'wb') as f:
        pickle.dump(docs_df, f)
        logger.info(f'DataFrame with realtime dataset documents serialized and stored in {const.REALDATA_PICKLE}')


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag_mon'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    extract_realtime_data(None, current_ts, MockDag)
