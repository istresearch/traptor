import os
from functools import wraps
from dog_whistle import dw_config, dw_callback
from scutils.log_factory import LogFactory
from traptor import settings
from birdy.twitter import AppClient
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

client = None

def _get_twitter():
    """Create a connection to Twitter"""
    global client
    if not client:
        client = AppClient(settings.APIKEYS['CONSUMER_KEY'], settings.APIKEYS['CONSUMER_SECRET'])
        client.get_access_token()
    logger.info(CONNECT_TO_TWITTER)
    return client

def status():
    _get_twitter()
    response = { }
    try:
        response['status'] = 'ok' if client.access_token != None else 'error'
    except Exception as e:
        response['status'] = 'error'
        response['detail'] = str(e)
    status_code = 200 if response['status'] == 'ok' else 500
    logger.info(API_STATUS, extra={'response': response, 'status_code': status_code})
    return response, status_code

def validate(rule):
    _get_twitter()
    response = {}
    response['rule'] = rule
    try:
        if rule['type'] in ('username'):
            response['result'] = _validate_follow_rule(rule['value'])
        if rule['type'] in ('keyword', 'hashtag'):
            response['result'] = _validate_track_rule(rule['value'])
        if rule['type'] in ('geo'):
            response['result'] = _validate_geo_rule(rule['value'])
        status_code = 200
    except Exception as e:
        response['result'] = {'error': str(e)}
        status_code = 500
    logger.info(VALIDATE_RULE, extra={'request': rule, 'response': response, 'status_code': status_code})
    return response, status_code

def _validate_follow_rule(value):
    response = {}
    if value[0] == '@':
        value = value[1:]
    try:
        response['userid'] = client.api.users.show.get(screen_name=value).data.id_str
        response['status'] = 'ok'
    except Exception as e:
        response['status'] = 'error'
        response['error'] = str(e)
    return response

def _validate_track_rule(value):
    return {'valid': True, 'value': value }

def _validate_geo_rule(value):
    return {'valid': True, 'value': value }

