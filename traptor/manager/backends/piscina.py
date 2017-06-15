import json
from urllib3 import ProxyManager, make_headers
from traptor import settings

USERS_LOOKUP_STR = settings.TWITTER_API_URL + '/users/lookup.json?screen_name={}'
SEARCH_TWEETS_STR = settings.TWITTER_API_URL + '/search/tweets.json?q={}&result_type=recent&count=100&include_entities=false'

def get_userid_for_username(username):
    http = ProxyManager(settings.PROXY_URL, proxy_headers=make_headers(proxy_basic_auth='{}:{}'.format(settings.PROXY_USER, settings.PROXY_PASSWORD)))
    resp = http.request('GET', USERS_LOOKUP_STR.format(username))
    assert resp.status == 200
    data = json.loads(resp.data)
    return data[0]['id_str']

def get_recent_tweets_by_keyword(keyword):
    http = ProxyManager(settings.PROXY_URL, proxy_headers=make_headers(proxy_basic_auth='{}:{}'.format(settings.PROXY_USER, settings.PROXY_PASSWORD)))
    resp = http.request('GET', SEARCH_TWEETS_STR.format(keyword))
    assert resp.status == 200
    data = json.loads(resp.data)
    return data
