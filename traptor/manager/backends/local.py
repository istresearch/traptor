import os
from birdy.twitter import AppClient
from traptor import settings

client = None
def _get_twitter():
    """Create a connection to Twitter"""
    global client
    if not client or not client.access_token:
        client = AppClient(os.getenv('CONSUMER_KEY', settings.APIKEYS['CONSUMER_KEY']), os.getenv('CONSUMER_SECRET', settings.APIKEYS['CONSUMER_SECRET']))
        client.get_access_token()
    return client

def get_userid_for_username(username):
    _get_twitter()    
    data = client.api.users.show.get(screen_name=username).data
    return data.id_str

def get_recent_tweets_by_keyword(keyword):
    _get_twitter()    
    data = client.api.search.tweets.get(q=keyword, result_type='recent', count=100, include_entities='false').data
    return data
