import contextlib
import logging
import os

logging.basicConfig(level=logging.INFO)
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

