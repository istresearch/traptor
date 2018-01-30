import os
from birdy.twitter import AppClient, TwitterClientError
from traptor import settings
from api import logger
from time import sleep

def retry_on_error(func):
    def func_wrapper(*args, **kwargs):
        retries = 0

        while retries < settings.TWITTERAPI_RETRY:
            try:
                return func(*args, **kwargs)
            except TwitterClientError as e:
                ex_str = str(e)
                # the TwitterClientError is just a string cast of a lower level exception
                if ex_str.find("Connection aborted") > -1:
                    retries += 1
                    logger.debug("Connection aborted, retrying {}/{}".format(retries, settings.TWITTERAPI_RETRY))
                    sleep(.05)

                    if retries == settings.TWITTERAPI_RETRY:
                        raise
                else:
                    raise

            return None
    return func_wrapper

client = None
def _get_twitter():
    """Create a connection to Twitter"""
    global client
    if not client or not client.access_token:
        client = AppClient(os.getenv('CONSUMER_KEY', settings.APIKEYS['CONSUMER_KEY']), os.getenv('CONSUMER_SECRET', settings.APIKEYS['CONSUMER_SECRET']))
        client.get_access_token()
    return client

@retry_on_error
def get_userid_for_username(username):
    _get_twitter()    
    data = client.api.users.show.get(screen_name=username).data
    return data.id_str

@retry_on_error
def get_recent_tweets_by_keyword(keyword):
    _get_twitter()    
    data = client.api.search.tweets.get(q=keyword, result_type='recent', count=100, include_entities='false').data
    return data
