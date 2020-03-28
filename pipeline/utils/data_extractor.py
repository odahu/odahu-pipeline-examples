import logging
import os
import tarfile

from utils.const import BUCKET
from utils.misc import chdir

logger = logging.getLogger(__name__)
from google.cloud import storage


def download_tar_gz(path: str, where: str):
    """
    Download tar.gz file from Google Storage and extract it
    :param path: gs tar gz archive
    :param where: where to download and unpack archive
    :return:
    """
    with chdir(where):
        client = storage.Client()
        bucket = client.get_bucket(BUCKET)
        base_name = os.path.basename(path)
        logger.info(f'File will be downloaded into {base_name}')

        # download
        with open(base_name, mode='wb') as f:
            blob = bucket.get_blob(path)
            blob.download_to_file(f)

        # extract
        tarfile.open(base_name, 'r:gz').extractall('.')
        logger.info("done.")
        return where





