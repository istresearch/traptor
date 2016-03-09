"""Traptor tests."""

import json
import redis
import pytest
from mock import MagicMock

from traptor.traptor import Traptor, MyBirdyClient
from traptor.settings import (KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID,
                              TRAPTOR_TYPE, REDIS_HOST, REDIS_PORT, REDIS_DB,
                              REDIS_PUBSUB_CHANNEL)
from scripts.rule_extract import RulesToRedis
from scutils.log_factory import LogObject


@pytest.fixture()
def redis_rules(request):
    """Load up some sample traptor rules into Redis."""
    with open('tests/data/track_rules.json') as f:
        track_rules = [json.loads(line) for line in f]
    with open('tests/data/follow_rules.json') as f:
        follow_rules = [json.loads(line) for line in f]
    with open('tests/data/locations_rules.json') as f:
        locations_rules = [json.loads(line) for line in f]

    conn = redis.StrictRedis(host=REDIS_HOST, port=6379, db=5)
    conn.flushdb()

    rc = RulesToRedis(conn)
    rc.send_rules(traptor_type='track', rules=track_rules)
    rc.send_rules(traptor_type='follow', rules=follow_rules)
    rc.send_rules(traptor_type='locations', rules=locations_rules)

    def cleanup():
        conn.flushdb()

    request.addfinalizer(cleanup)

    return conn


@pytest.fixture()
def pubsub_conn():
    """Create a connection to the Redis PubSub."""
    p_conn = redis.StrictRedis(host=REDIS_HOST, port=6379, db=5)
    return p_conn


@pytest.fixture()
def traptor_notify_channel():
    """Create a traptor notification channel."""
    return 'traptor-notify'


@pytest.fixture(params=['track',
                        'follow',
                        'locations',
                        ])
def traptor(request, redis_rules, pubsub_conn, traptor_notify_channel):
    """Create a Traptor instance."""
    traptor_instance = Traptor(redis_conn=redis_rules,
                               pubsub_conn=pubsub_conn,
                               traptor_type=request.param,
                               apikeys=APIKEYS,
                               traptor_id=0,
                               kafka_hosts=KAFKA_HOSTS,
                               kafka_topic='traptor_test',
                               kafka_enabled=False,
                               log_level='INFO',
                               test=True,
                               traptor_notify_channel=traptor_notify_channel
                               )

    return traptor_instance


@pytest.fixture
def tweets(request, traptor):
    """Create a list of tweets."""
    with open('tests/data/' + traptor.traptor_type + '_tweet.json') as f:
        loaded_tweet = json.load(f)

    return loaded_tweet,


@pytest.fixture
def pubsub_messages(request):
    """Create a list of pubsub messages."""
    with open('tests/data/pubsub_messages.txt') as f:
        messages = [line for line in f]

    return messages,


class TestRuleExtract():
    """Test the rule extraction functionality."""

    def test_track(self, redis_rules):
        """Test retrieving the tracking rules."""
        assert {'tag': 'test', 'value': 'happy'} == redis_rules.hgetall('traptor-track:0:0')

    def test_follow(self, redis_rules):
        """Test retrieving the follow rules."""
        assert {'tag': 'test', 'value': '17919972'} == redis_rules.hgetall('traptor-follow:0:0')

    def test_locations(self, redis_rules):
        """Test retrieving the location rules."""
        assert {'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8'} == redis_rules.hgetall('traptor-locations:0:0')


class TestTraptor(object):
    """
    Traptor tests.

    Test Traptor running in test mode, then test all the methods individually.

    TODO:  Add Kafka testing.
    TODO:  Add location testing.
    """

    @pytest.mark.timeout(15)
    def test_traptor_run(self, traptor, tweets):
        """Ensure Traptor can be run."""
        traptor.birdy_stream = MagicMock(return_value=tweets)
        traptor.birdy_stream.stream = traptor.birdy_stream
        # TODO: get this working with the function started in a thread
        # traptor.run()

    @pytest.mark.timeout(15)
    def test_setup(self, traptor):
        """Ensure we can set up a Traptor."""
        traptor._setup()

        assert isinstance(traptor.logger, LogObject)
        assert isinstance(traptor.birdy_conn, MyBirdyClient)

    @pytest.mark.timeout(15)
    def test_redis_rules(self, redis_rules, traptor):
        """Ensure the correct rules are retrieved for the Traptor type."""
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]

        if traptor.traptor_type == 'track':
            assert traptor.redis_rules == [{'tag': 'test', 'value': 'happy'}]
        if traptor.traptor_type == 'follow':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '17919972'}]
        if traptor.traptor_type == 'locations':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8'}]

    @pytest.mark.timeout(15)
    def test_twitter_rules(self, traptor):
        """Ensure Traptor can create Twitter rules from the Redis rules."""
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

        if traptor.traptor_type == 'track':
            assert traptor.twitter_rules == 'happy'
        if traptor.traptor_type == 'follow':
            assert traptor.twitter_rules == '17919972'
        if traptor.traptor_type == 'locations':
            assert traptor.twitter_rules == '-122.75,36.8,-121.75,37.8'

    @pytest.mark.timeout(15)
    def test_main_loop(self, traptor, tweets):
        """Ensure we can loop through the streaming Twitter data."""
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
            # assert enriched_data['traptor']['rule_value'] == \
            #    '-122.75,36.8,-121.75,37.8'

    @pytest.mark.timeout(15)
    def test_check_redis_pubsub_for_restart(self, traptor, pubsub_conn):
        """Test pubsub message causes the restart_flag to be set to True."""
        traptor._setup()
        traptor.pubsub_conn = pubsub_conn
        traptor.restart_flag = True
        # Run the _check_redis_pubsub_for_restart function
        traptor._check_redis_pubsub_for_restart()
        assert True
