import contextlib
import logging
import subprocess
import tempfile
from functools import wraps
from subprocess import PIPE

from airflow import DAG

from utils import const
from utils.misc import chdir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_workspace_path(execution_date, dag_id):
    logger.info(f'Execution date: {execution_date}')
    return f'gs://{const.BUCKET}/dag_workspaces/{dag_id}/{execution_date}'


@contextlib.contextmanager
def workspace(bucket_ws_path):
    """
    Used to sync data between tasks on different workers via bucket

    On Enter:
        1. Create temp dir
        2. Set temp dir as working directory
        3. Fetch all data from remote `get_workspace_path()` bucket path to this directory
    On Exit:
        1. Upload -r all data from temp directory to bucket
        2. Unset created temp directory as working directory
    :param bucket_ws_path: remote workspace (in bucket)
    :return:
    """
    with tempfile.TemporaryDirectory() as folder:
        logger.info(f'Create working directory at {folder}')
        logger.info(f'Remote folder in storage for sync: {bucket_ws_path}')
        with chdir(folder):
            try:
                # we use subprocess instead of Python SDK because the latter is not has simple api to recursively
                # fetch entire directory
                logger.info(f'Bucket -> Local workspace sync started')
                result = subprocess.run(['gsutil', 'cp', '-r', f'{bucket_ws_path}/*', './'], stdout=PIPE, stderr=PIPE)
                logger.info(f'return code: {result.returncode}, stderr: {result.stderr}, stdout: {result.stdout}')
                if result.returncode != 0:
                    logger.info(f'return code: {result.returncode}, stderr: {result.stderr}')
                logger.info(f'Bucket -> Local workspace synced')
                yield
                logger.info(f'Local workspace -> Bucket sync started')
                result = subprocess.run(['gsutil', 'cp', '-r', './*', f'{bucket_ws_path}/'], stdout=PIPE, stderr=PIPE)
                if result.returncode != 0:
                    logger.info(f'return code: {result.returncode}, stderr: {result.stderr}')
                logger.info(f'Local workspace -> Bucket synced')
            finally:
                pass
        logger.info(f'Working directory at {folder} will be removed')


def inside_workspace(func):
    """
    Decorated function will be executed in workspace temp directory that is synced with remote bucket
    All new data automatically uploaded to bucket after function is finished
    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(ds, ts, dag: DAG, **kwargs):
        with workspace(get_workspace_path(ts, dag.dag_id)):
            return func(ds, ts, dag, **kwargs)

    return wrapper
