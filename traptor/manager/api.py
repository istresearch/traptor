import os
import settings
from dog_whistle import dw_config, dw_callback
from functools import wraps
from scutils.log_factory import LogFactory
from twython import Twython
from __strings__ import *

# Initialize Logging

logger = LogFactory.get_instance(name=os.getenv('LOG_NAME', settings.LOG_NAME),
            json=os.getenv('LOG_JSON', settings.LOG_JSON) == 'True',
            stdout=os.getenv('LOG_STDOUT', settings.LOG_STDOUT) == 'True',
            level=os.getenv('LOG_LEVEL', settings.LOG_LEVEL),
            dir=os.getenv('LOG_DIR', settings.LOG_DIR),
            file=os.getenv('LOG_FILE', settings.LOG_FILE))

if settings.DW_ENABLED:
    dw_config(settings.DW_CONFIG)
    logger.register_callback('>=INFO', dw_callback)

def status():
    logger.info(API_STATUS)
    response = { }
    try:
        response['status'] = 'ok' if _connect_to_twitter().access_token is not None else 'error'
    except Exception as e:
        response['status'] = 'error'
        response['detail'] = str(e)
    return response, 200

def _connect_to_twitter():
    """Create a connection to Twitter."""
    logger.info(CONNECT_TO_TWITTER)
    twitter = Twython(settings.APIKEYS['CONSUMER_KEY'],
                      settings.APIKEYS['CONSUMER_SECRET'],
                      oauth_version=2)
    access_token = twitter.obtain_access_token()
    twitter = Twython(settings.APIKEYS['CONSUMER_KEY'], access_token=access_token)

    return twitter
