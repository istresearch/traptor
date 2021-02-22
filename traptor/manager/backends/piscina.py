import json
from urllib3 import ProxyManager, make_headers
from traptor import settings

USERS_LOOKUP_STR = settings.TWITTER_API_URL + '/users/lookup.json'
SEARCH_TWEETS_STR = settings.TWITTER_API_URL + '/search/tweets.json'

def _get_proxy():
    return ProxyManager(settings.PROXY_URL, proxy_headers=make_headers(proxy_basic_auth='{}:{}'.format(settings.PROXY_USER, settings.PROXY_PASSWORD)))

def get_screen_name_for_userid(userid):
    http = _get_proxy()

    fields = {
        'user_id': userid,
    }

    resp = http.request('GET', USERS_LOOKUP_STR, fields, timeout=settings.PROXY_TIMEOUT)
    assert resp.status == 200, 'Twitter API error ({}): {}'.format(resp.status, resp.data)
    data = json.loads(resp.data)
    return data[0]['screen_name']

def get_userid_for_username(username):
    http = _get_proxy()

    fields = {
        'screen_name': username,
    }

    resp = http.request('GET', USERS_LOOKUP_STR, fields, timeout=settings.PROXY_TIMEOUT)
    assert resp.status == 200, 'Twitter API error ({}): {}'.format(resp.status, resp.data)
    data = json.loads(resp.data)
    return data[0]['id_str']

def get_recent_tweets_by_keyword(keyword):
    http = _get_proxy()

    fields = {
        'q': keyword,
        'result_type': 'recent',
        'count': 100,
        'include_entities': 'false'
    }

    resp = http.request('GET', SEARCH_TWEETS_STR, fields, timeout=settings.PROXY_TIMEOUT)
    assert resp.status == 200, 'Twitter API error ({}): {}'.format(resp.status, resp.data)
    data = json.loads(resp.data)
    return data
