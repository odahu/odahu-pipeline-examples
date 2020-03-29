import logging
import os

__APP_PREFIX = 'ODAHU_REUTERS_PIPELINE'

# path to local directory that will be synced with remote bucket (if WORKSPACE_SYNC=True) while using
# @inside_workspace decorator
# If not set – temp directory will be created, and removed after decorated function finished
WORKSPACE = os.environ.get(f'{__APP_PREFIX}__WORKSPACE')

# If True – local workspace will be synced with remote bucket on start  and finish every decorated function
WORKSPACE_SYNC = int(os.environ.get(f'{__APP_PREFIX}__WORKSPACE_SYNC', 1))

# path to ODAHU credentials in json format:
# DESCRIBE FORMAT
# If not set – Airflow connection will be used
ODAHU_CRED = os.environ.get(f'{__APP_PREFIX}__ODAHU_CRED')

# set basic log level as info
logging.basicConfig(level=logging.INFO)
