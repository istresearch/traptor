import dateutil.parser as parser
import os
import traceback
from functools import wraps
from dog_whistle import dw_config, dw_callback
from scutils.log_factory import LogFactory
from traptor import settings
from __strings__ import *

if settings.API_BACKEND == 'piscina':
    from backends.piscina import get_userid_for_username, get_recent_tweets_by_keyword
else: 
    from backends.local import get_userid_for_username, get_recent_tweets_by_keyword

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

def validate(rule):
    response = {}
    response['context'] = rule
    try:
        if rule['type'] in ('username',):
            result = _validate_follow_rule(rule['value'])
        if rule['type'] in ('keyword', 'hashtag'):
            result = _validate_track_rule(rule['value'])
        if rule['type'] in ('geo',):
            result = _validate_geo_rule(rule['value'])
        for kk in result:
            response[kk] = result[kk]
        status_code = 200 if result['status'] == 'ok' else 500
    except Exception as e:
        response['result'] = {'status': 'error', 'error': str(e), 'detail': traceback.format_exc()}
        status_code = 500
    logger.info(VALIDATE_RULE, extra={'request': rule, 'response': response, 'status_code': status_code})
    return response, status_code

def _validate_follow_rule(value):
    response = {}
    if value[0] == '@':
        value = value[1:]
    try:
        response['userid'] = get_userid_for_username(value)
        response['status'] = 'ok'
    except Exception as e:
        response['error'] = str(e)
        response['status'] = 'error'
    return response

def _validate_track_rule(value):
    response = {}
    search_results = get_recent_tweets_by_keyword(value)
    tweet_creation_times = []
    for result in search_results['statuses']:
        new_dt = parser.parse(result['created_at'], ignoretz=True)
        tweet_creation_times.append(new_dt)
    tweet_creation_times = sorted(tweet_creation_times)
    if len(tweet_creation_times) == 1:
        response['tweets_per_second'] = 0
        response['status'] = 'ok'
    elif len(tweet_creation_times) > 1:
        first_tweet = min(tweet_creation_times)
        last_tweet = max(tweet_creation_times)
        time_diff_in_seconds = (last_tweet - first_tweet).total_seconds()
        total_tweets = float(len(tweet_creation_times))
        try:
            response['tweets_per_second'] = float(total_tweets / time_diff_in_seconds)
            response['status'] = 'ok'
        except ZeroDivisionError as e:
            # tweets all arrived at the same time -> INFINITE TWEETS PER SECOND!!!
            logger.error("Zero division error occurred when calculating tweets per second", extra={'exception': str(e)})
            response['tweets_per_second'] = 10000
            response['status'] = 'ok'
        except Exception as e:
            # general edge case coverage - let it through
            logger.error("Error when calculating tweets per second", extra={'exception': str(e)})
            response['tweets_per_second'] = 0
            response['status'] = 'ok'
    else:
        response['tweets_per_second'] = 0
        response['status'] = 'ok'
    return response

def _validate_geo_rule(value):
    response = {}
    if isinstance(value, basestring):
        try:    
            lon1, lat1, lon2, lat2 = value.split(',')
            response['status'] = 'ok'
            for lon, lat in ((lon1, lat1), (lon2, lat2)):
                if not ((-90 <= float(lat) <= 90) and (-180 <= float(lon) <= 180)):
                    response['status'] = 'error'
                    response['error'] = 'Lat/lon out of range: {}, {}'.format(lat, lon)
                    logger.warn('Lat/lon out of range', extra={'lat': lat, 'lon': lon})
            if response['status'] == 'ok':
                response['geo_type'] = 'legacy'
        except Exception as e:
            response['status'] = 'error'
            response['error'] = str(e)
    if isinstance(value, dict):
        try:    
            type = value.get('type')
            if not type or type.lower() != 'feature':
                raise Exception('GeoJSON type not supported: {}'.format(type))
            geometry = value.get('geometry')
            if not isinstance(geometry, dict):  
                raise Exception('Invalid GeoJSON object')
            geo_type = geometry.get('type')
            if not geo_type or geo_type.lower() != 'polygon':
                raise Exception('GeoJSON geometry type not supported: {}'.format(type))
            coord_list = geometry.get('coordinates')
            if coord_list is None or len(coord_list) == 0:
                raise Exception('GeoJSON coordinates list is empty.')
            for linear_ring in coord_list:
                if not isinstance(linear_ring, list):
                    raise Exception('Linear ring in coordinates is not a list')
                if not len(linear_ring):
                    raise Exception('Linear ring in coordinates is empty')
                deduped = [linear_ring[i] for i in range(len(linear_ring)) if i == 0 or linear_ring[i] != linear_ring[i-1]]
                for lon, lat in deduped:
                    if not ((-90 <= float(lat) <= 90) and (-180 <= float(lon) <= 180)):
                        raise Exception('Lat/lon out of range: {}, {}'.format(lat, lon))
                        logger.warn('Lat/lon out of range', extra={'lat': lat, 'lon': lon})
                del linear_ring[:]
                linear_ring.extend(deduped)
                if len(linear_ring) >= 3:
                    if cmp(linear_ring[0], linear_ring[-1]) != 0:
                        linear_ring.append(list(linear_ring[0]))
                if 0 <= len(linear_ring) < 4:
                    raise Exception('Linear ring in coordinates is too short')
            response['status'] = 'ok'
            response['geo_type'] = 'geo_json'
        except Exception as e:
            response['status'] = 'error'
            response['error'] = str(e)
    return response
