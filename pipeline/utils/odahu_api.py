import json
import logging
from typing import Dict

import requests

from utils import config, const
from airflow.hooks.base_hook import BaseHook
from __version__ import VERSIONED_PROJECT

api_connection_id = "odahuflow_api"

logger = logging.getLogger(__name__)


def _get_token(extra: Dict) -> str:
    """
    Authorize test user and get access token.

    :param Airlfow API connection
    :return: access token
    """

    auth_url = extra["auth_url"]

    try:
        response = requests.post(
            auth_url,
            data={
                'grant_type': 'client_credentials',
                'client_id': extra["client_id"],
                'client_secret': extra["client_secret"],
                'scope': extra['scope']
            }
        )
        response_data = response.json()

        # Parse fields and return
        id_token = response_data.get('id_token')
        token_type = response_data.get('token_type')
        expires_in = response_data.get('expires_in')

        logger.info('Received %s token with expiration in %d seconds', token_type, expires_in)

        return id_token
    except requests.HTTPError as http_error:
        raise Exception(f'Can not authorize user {extra["client_id"]} on {auth_url}: {http_error}') from http_error


def get_token() -> str:
    if config.ODAHU_CRED:
        with open(config.ODAHU_CRED, 'rt') as f:
            extra = json.load(f)
    else:
        conn = BaseHook.get_connection(api_connection_id)
        extra = conn.extra
    token = _get_token(extra)
    return token


def get_model_uri() -> str:
    return f'https://{const.API_HOST}/model/{VERSIONED_PROJECT}/api/model/invoke'


def predict(json_: Dict):
    res = requests.post(get_model_uri(), data=json_, headers={
        'Authorization': f'Bearer {get_token()}',
        'Content-Type': 'application/json',
        'accept': 'application/json'
    })
    res.raise_for_status()
    return res.json()



