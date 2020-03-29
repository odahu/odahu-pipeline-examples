import logging
from datetime import datetime

from airflow import DAG
from google.cloud import storage

from utils.workspace import inside_workspace
from utils import const

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@inside_workspace()
def prepare_odahu_training(ds, ts, dag: DAG, **kwargs):
    """
    Extract data from external storage (e.g. Company Data Lake) and upload it to bucket
    :param ds:
    :param kwargs:
    :return:
    """
    client = storage.Client()
    bucket = client.get_bucket(const.BUCKET)
    blob = bucket.blob(f'{const.TRAINING_INPUT_DIR}/{const.TOPICS_PICKLE_FILE}')
    blob.upload_from_filename(const.TOPICS_PICKLE_FILE)
    blob = bucket.blob(f'{const.TRAINING_INPUT_DIR}/{const.DATASET_PICKLE_FILE}')
    blob.upload_from_filename(const.DATASET_PICKLE_FILE)


if __name__ == '__main__':

    class MockDag:

        dag_id = 'local_dag'

    current_ts = 'local_ts'
    logger.info(f'current ts: {current_ts}')
    prepare_odahu_training(None, current_ts, MockDag)
