"""Traptor tests."""
# To run with autotest and coverage and print all output to console run:
#   py.test -s --cov=traptor --looponfail tests/

import json
import time
from datetime import datetime
from redis import StrictRedis, ConnectionError
import pytest
from mock import MagicMock

from traptor.traptor import Traptor, MyBirdyClient
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

    conn = StrictRedis(host='scdev', port=6379, db=5)
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
    """Create a connection for the Redis PubSub."""
    p_conn = StrictRedis(host='scdev', port=6379, db=5)
    return p_conn


@pytest.fixture()
def traptor_notify_channel():
    """Create a traptor notification channel."""
    return 'traptor-notify'


@pytest.fixture(params=['track',
                        'follow',
                        'locations',
                        ])
def traptor(request, redis_rules, pubsub_conn, heartbeat_conn, traptor_notify_channel):
    """Create a Traptor instance."""
    APIKEYS = ({
        'CONSUMER_KEY': '',
        'CONSUMER_SECRET': '',
        'ACCESS_TOKEN': '',
        'ACCESS_TOKEN_SECRET': ''
    })
    traptor_instance = Traptor(redis_conn=redis_rules,
                               pubsub_conn=pubsub_conn,
                               heartbeat_conn=heartbeat_conn,
                               traptor_type=request.param,
                               apikeys=APIKEYS,
                               traptor_id=0,
                               kafka_hosts='scdev:9092',
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

    return loaded_tweet


@pytest.fixture
def pubsub_messages(request):
    """Create a list of pubsub messages."""
    with open('tests/data/pubsub_messages.txt') as f:
        messages = [line for line in f]

    return messages


@pytest.fixture()
def heartbeat_conn():
    """Create a connection for the heartbeat."""
    hb_conn = MagicMock()
    return hb_conn


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

    def test_setup(self, traptor):
        """
            Ensure we can set up a Traptor.
            Covers the following methods:

            _setup_birdy()
            _setup_kafka() [Kafka connection expected to be None in offline mode]
            _setup()
        """
        traptor._setup()

        assert isinstance(traptor.logger, LogObject)
        assert traptor.restart_flag is False
        assert traptor.kafka_conn is None
        assert isinstance(traptor.birdy_conn, MyBirdyClient)

    def test_create_kafka_producer(self, traptor):
        """Ensure we aren't connecting to a Kafka topic that doesn't exist."""
        traptor._setup()
        traptor._create_kafka_producer('testtopic')
        assert traptor.kafka_producer == None


    # def test_create_birdy_stream(self):
    #     pass

    # def test_make_twitter_rules(self):
    #     pass

    # def test_add_rule_tag_and_value_to_tweet(self):
    #     pass

    # def test_find_rule_matches(self):
    #     pass

    # def test_get_redis_rules(self):
    #     pass

    # def test_tweet_time_to_iso(self):
    #     pass

    # def test_create_traptor_obj(self):
    #     pass

    # def test_fix_tweet_object(self):
    #     pass

    def test_check_redis_pubsub_for_restart(self, traptor, pubsub_conn):
        """Test pubsub message causes the restart_flag to be set to True."""
        traptor._setup()
        traptor.pubsub_conn = pubsub_conn
        traptor.restart_flag = True
        # Run the _check_redis_pubsub_for_restart function
        traptor._check_redis_pubsub_for_restart()
        assert True

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

    def test_main_loop(self, traptor, tweets):
        """Ensure we can loop through the streaming Twitter data."""
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)
        traptor.birdy_stream = MagicMock(return_value=tweets)
        traptor.birdy_stream.stream = traptor.birdy_stream

        _data = traptor.birdy_stream.stream()
        data = traptor._fix_tweet_object(_data)
        enriched_data = traptor._find_rule_matches(data)

        if traptor.traptor_type == 'track':

            assert data['traptor']['created_at_iso'] == '2016-02-22T01:34:53+00:00'
            assert enriched_data['traptor']['rule_tag'] == 'test'
            assert enriched_data['traptor']['rule_value'] == 'happy'

        if traptor.traptor_type == 'follow':
            assert data['traptor']['created_at_iso'] == '2016-02-20T03:52:59+00:00'
            assert enriched_data['traptor']['rule_tag'] == 'test'
            assert enriched_data['traptor']['rule_value'] == '17919972'

        if traptor.traptor_type == 'locations':
            assert data['traptor']['created_at_iso'] == '2016-02-23T02:02:54+00:00'

            # TODO.
            # Need to figure out how to map location rules back to results.
            # Need to do some coordinate math on the geo bounding boxes.

            # assert enriched_data['traptor']['rule_tag'] == 'test'
            # assert enriched_data['traptor']['rule_value'] == \
            #    '-122.75,36.8,-121.75,37.8'

    def test_ensure_heartbeat_message_is_produced(self, traptor):
        """Ensure Traptor can produce heartbeat messages."""
        traptor._setup()

        traptor.heartbeat_conn = MagicMock()
        traptor._add_heartbeat_message_to_redis = MagicMock(return_value=1)

        result = traptor._add_heartbeat_message_to_redis(traptor.heartbeat_conn,
                                                         'track',
                                                         '0')
        assert result == 1

    def test_ensure_heartbeat_raises_error_if_encountered(self, traptor):
        """Ensure Traptor handles Redis connection issues when producing a heartbeat."""
        with pytest.raises(ConnectionError):
            traptor._setup()

            traptor.heartbeat_conn = MagicMock()
            traptor.heartbeat_conn.setex.side_effect = ConnectionError

            traptor._add_heartbeat_message_to_redis(traptor.heartbeat_conn)
