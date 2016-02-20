import json

import redis
import pytest
from mock import MagicMock
from click.testing import CliRunner

from traptor.traptor import Traptor
from traptor.settings import APIKEYS
from scripts.rule_extract import RulesToRedis
from scutils.log_factory import LogFactory


@pytest.fixture()
def redis_rules(request):
    """ Load up some sample traptor rules into Redis. """

    with open('tests/track_rules.json') as f:
        track_rules = [json.loads(line) for line in f]
    with open('tests/follow_rules.json') as f:
        follow_rules = [json.loads(line) for line in f]

    conn = redis.StrictRedis(host='localhost', port=6379, db=5)
    conn.flushall()

    rc = RulesToRedis(conn)
    rc.send_rules(traptor_type='track', rules=track_rules)
    rc.send_rules(traptor_type='follow', rules=follow_rules)

    def cleanup():
        conn.flushall()

    request.addfinalizer(cleanup)

    return conn


@pytest.fixture(params=['track',
                        'follow',
                        ])
def traptor(request, redis_rules):
    traptor_instance = Traptor(apikeys=APIKEYS,
                               traptor_type=request.param,
                               traptor_id=0,
                               kafka_hosts='localhost',
                               kafka_topic='traptor_test',
                               redis_conn=redis_rules,
                               kafka_enabled=False,
                               log_level='INFO',
                               test=True
                               )

    return traptor_instance


@pytest.fixture()
def tweets(request):
    with open('tests/twitter_tweet.json') as f:
        twitter_tweet = json.load(f)
    with open('tests/twitter_retweet.json') as f:
        twitter_retweet = json.load(f)

    return twitter_tweet, twitter_retweet


class TestRuleExtract():
    def test_track(self, redis_rules):

        assert {'tag': 'test', 'value': 'happy'} == redis_rules.hgetall('traptor-track:0:0')
        assert {'tag': 'test', 'value': 'love people'} == redis_rules.hgetall('traptor-track:0:1')

    def test_follow(self, redis_rules):

        assert {'tag': 'test', 'value': '1630833338'} == redis_rules.hgetall('traptor-follow:0:0')


class TestTrackTraptor(object):
    # @vcr.use_cassette()
    def test_setup(self, traptor, tweets):

        for i in tweets:
            assert 'id' in i
        # traptor.birdy_stream = MagicMock(return_value=tweets)
        # traptor.run()
