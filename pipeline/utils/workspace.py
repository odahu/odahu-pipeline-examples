import contextlib
import logging
import tempfile
from functools import wraps

from airflow import DAG

from utils import const
from utils.misc import chdir, gcs_sync
from utils import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_workspace_path(execution_date, dag_id):
    return f'gs://{const.BUCKET}/dag_workspaces/{dag_id}/{execution_date}'


@contextlib.contextmanager
def workspace_folder():
    try:
        if config.WORKSPACE:
            logger.info(f'Persistent workspace is set – {config.WORKSPACE}.')
            yield config.WORKSPACE
        else:
            with tempfile.TemporaryDirectory() as folder:
                logger.info(f'Temp workspace will be created – {folder}')
                yield folder
                logger.info(f'Temp workspace will be removed – {folder}')
    finally:
        pass


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
    with workspace_folder() as folder:
        with chdir(folder):
            try:
                if config.WORKSPACE_SYNC:
                    gcs_sync(f'{bucket_ws_path}/*', './')
                else:
                    logger.warning('Workspace sync is disabled by configuration')
                yield
                if config.WORKSPACE_SYNC:
                    gcs_sync('./*', f'{bucket_ws_path}/')
            finally:
                pass


def inside_workspace():
    """
    Decorated function will be executed in workspace temp directory that is synced with remote bucket
    All new data automatically uploaded to bucket after function is finished
    :return:
    """

    def inner(func):

        @wraps(func)
        def wrapper(ds, ts, dag: DAG, **kwargs):
            with workspace(get_workspace_path(ts, dag.dag_id)):
                return func(ds, ts, dag, **kwargs)

        return wrapper

    return inner
