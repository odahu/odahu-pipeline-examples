import logging
import os
import pickle

import pandas as pd
from airflow import DAG

from utils import const
from utils.data_extractor import download_tar_gz
from utils.parser import get_topics_from_reuters_documents, stream_reuters_body_topics
from contextlib import suppress

from utils.workspace import inside_workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace
def prepare_base_dataset(ds, ts, dag: DAG, **kwargs):
    """
    Extract base data set (articles in XML format) and parse it to DataFrame. Store in workspace
    :param ds:
    :param kwargs:
    :return:
    """
    folder = 'base_dataset'
    with suppress(FileExistsError):
        os.mkdir(folder)
    download_tar_gz(f'data/base_dataset.tar.gz', folder)
    docs = list(stream_reuters_body_topics(folder))
    docs_df = pd.DataFrame(data=docs, columns=['text', 'topics'])
    topics = list(get_topics_from_reuters_documents(folder))

    with open(const.DATASET_PICKLE_FILE, 'wb') as f:
        pickle.dump(docs_df, f)
        logger.info(f'DataFrame with base dataset documents serialized and stored in {const.DATASET_PICKLE_FILE}')
    with open(const.TOPICS_PICKLE_FILE, 'wb') as f:
        pickle.dump(topics, f)
        logger.info(f'topics list serialized and stored in {const.TOPICS_PICKLE_FILE}')


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    prepare_base_dataset(None, current_ts, MockDag)
