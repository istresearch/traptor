import json

import redis
import pytest
from mock import MagicMock
from click.testing import CliRunner

from traptor.traptor import Traptor, MyBirdyClient
from traptor.settings import APIKEYS
from scripts.rule_extract import RulesToRedis
from scutils.log_factory import LogFactory, LogObject


@pytest.fixture()
def redis_rules(request):
    """ Load up some sample traptor rules into Redis. """

    with open('tests/data/track_rules.json') as f:
        track_rules = [json.loads(line) for line in f]
    with open('tests/data/follow_rules.json') as f:
        follow_rules = [json.loads(line) for line in f]
    with open('tests/data/locations_rules.json') as f:
        locations_rules = [json.loads(line) for line in f]

    conn = redis.StrictRedis(host='localhost', port=6379, db=5)
    conn.flushall()

    rc = RulesToRedis(conn)
    rc.send_rules(traptor_type='track', rules=track_rules)
    rc.send_rules(traptor_type='follow', rules=follow_rules)
    rc.send_rules(traptor_type='locations', rules=locations_rules)

    def cleanup():
        conn.flushall()

    request.addfinalizer(cleanup)

    return conn


@pytest.fixture(params=['track',
                        'follow',
                        'locations',
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


@pytest.fixture
def tweets(request, traptor):
    with open('tests/data/' + traptor.traptor_type + '_tweet.json') as f:
        loaded_tweet = json.load(f)

    return loaded_tweet,


class TestRuleExtract():
    def test_track(self, redis_rules):
        assert {'tag': 'test', 'value': 'happy'} == redis_rules.hgetall('traptor-track:0:0')

    def test_follow(self, redis_rules):
        assert {'tag': 'test', 'value': '17919972'} == redis_rules.hgetall('traptor-follow:0:0')

    def test_locations(self, redis_rules):
        assert {'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8'} == redis_rules.hgetall('traptor-locations:0:0')


class TestTraptor(object):
    """ Test Traptor running in test mode, then test all the methods
        individually.

        TODO:  Add Kafka testing.
        TODO:  Add location testing.
    """
    def test_traptor_run(self, traptor, tweets):

        traptor.birdy_stream = MagicMock(return_value=tweets)
        traptor.birdy_stream.stream = traptor.birdy_stream

        traptor.run()

    def test_setup(self, traptor):
        traptor._setup()

        assert isinstance(traptor.logger, LogObject)
        assert isinstance(traptor.birdy_conn, MyBirdyClient)

    def test_redis_rules(self, redis_rules, traptor):
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]

        if traptor.traptor_type == 'track':
            assert traptor.redis_rules == [{'tag': 'test', 'value': 'happy'}]
        if traptor.traptor_type == 'follow':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '17919972'}]
        if traptor.traptor_type == 'locations':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8'}]

    def test_twitter_rules(self, traptor):
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

        if traptor.traptor_type == 'track':
            assert traptor.twitter_rules == 'happy'
        if traptor.traptor_type == 'follow':
            assert traptor.twitter_rules == '17919972'
        if traptor.traptor_type == 'locations':
            assert traptor.twitter_rules == '-122.75,36.8,-121.75,37.8'

    def test_main_loop(self, traptor, tweets):
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)
        traptor.birdy_stream = MagicMock(return_value=tweets)
        traptor.birdy_stream.stream = traptor.birdy_stream

        _data = traptor.birdy_stream.stream()[0]
        data = traptor._fix_tweet_object(_data)

        if traptor.traptor_type == 'track':
            assert data['traptor']['created_at_iso'] == '2016-02-22T01:34:53+00:00'
            enriched_data = traptor._find_rule_matches(data)
            assert enriched_data['traptor']['rule_tag'] == 'test'
            assert enriched_data['traptor']['rule_value'] == 'happy'

        if traptor.traptor_type == 'follow':
            assert data['traptor']['created_at_iso'] == '2016-02-20T03:52:59+00:00'
            enriched_data = traptor._find_rule_matches(data)
            assert enriched_data['traptor']['rule_tag'] == 'test'
            assert enriched_data['traptor']['rule_value'] == '17919972'

        if traptor.traptor_type == 'locations':
            assert data['traptor']['created_at_iso'] == '2016-02-23T02:02:54+00:00'
            enriched_data = traptor._find_rule_matches(data)

            # TODO.
            # Need to figure out how to map location rules back to results.
            # Need to do some coordinate math on the geo bounding boxes.

            # assert enriched_data['traptor']['rule_tag'] == 'test'
            # assert enriched_data['traptor']['rule_value'] == '-122.75,36.8,-121.75,37.8'
