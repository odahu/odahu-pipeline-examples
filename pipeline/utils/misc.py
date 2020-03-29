import contextlib
import logging
import os
import subprocess
from subprocess import PIPE
from typing import List

from utils import config

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def chdir(new_dir):
    curdir = os.getcwd()
    try:
        logger.info(f'Change dir to {new_dir}')
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(curdir)
        logger.info(f'Change dir to {curdir} back')


def gcs_sync(from_path, to_path):
    """
    Sync data from remote bucket to local or otherwise
    Use gsutil cp -r under the hood
    We don't use Python  SDK because of a lot of verbose code are required to implement the same
    :param from_path: synced from there
    :param to_path: synced to there
    :return:
    """
    logger.info(f'{from_path} -> {to_path} sync started')
    result = subprocess.run(['gsutil', 'cp', '-r', from_path, to_path], stdout=PIPE, stderr=PIPE)
    logger.info(f'return code: {result.returncode}, stderr: {result.stderr}, stdout: {result.stdout}')
    if result.returncode != 0:
        logger.info(f'return code: {result.returncode}, stderr: {result.stderr}')
    logger.info(f'{from_path} -> {to_path} sync finished')


def harvest_json_in_tree(folder) -> List[str]:
    """
    Recursively walk tree and harvest full paths to json files into List
    :param folder: Path to base directory
    :return: List of full paths to json files in dir
    """
    jsons = []
    for bp, _, filenames in os.walk(folder):
        jsons += [
            os.path.join(bp, f) for f in filenames if  f.endswith('.json')
        ]
    return jsons