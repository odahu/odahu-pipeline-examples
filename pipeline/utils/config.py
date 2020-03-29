import logging
import os

__APP_PREFIX = 'ODAHU_REUTERS_PIPELINE'

# path to local directory that will be synced with remote bucket workspace while using @inside_workspace decorator
# If not set – temp directory will be created, and removed after decorated function finished
WORKSPACE = os.environ.get(f'{__APP_PREFIX}__WORKSPACE')

# path to ODAHU credentials in json format:
# DESCRIBE FORMAT
# If not set – Airflow connection will be used
ODAHU_CRED = os.environ.get(f'{__APP_PREFIX}__ODAHU_CRED')


# set basic log level as info
logging.basicConfig(level=logging.INFO)