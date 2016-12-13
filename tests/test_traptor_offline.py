"""Traptor unit tests."""
# To run with autotest and coverage and print all output to console run:
#   py.test -s --cov=traptor --looponfail tests/

import os
import json
from redis import StrictRedis, ConnectionError
import pytest
from mock import MagicMock

from traptor.traptor import Traptor, MyBirdyClient
from scripts.rule_extract import RulesToRedis
from scutils.log_factory import LogObject

HOST_FOR_TESTING = os.getenv('REDIS_HOST', 'localhost')


@pytest.fixture()
def redis_rules(request):
    """Load up some sample traptor rules into Redis."""
    with open('tests/data/track_rules.json') as f:
        track_rules = [json.loads(line) for line in f]
    with open('tests/data/follow_rules.json') as f:
        follow_rules = [json.loads(line) for line in f]
    with open('tests/data/locations_rules.json') as f:
        locations_rules = [json.loads(line) for line in f]

    conn = StrictRedis(host=HOST_FOR_TESTING, port=6379, db=5)
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
    p_conn = StrictRedis(host=HOST_FOR_TESTING, port=6379, db=5)
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
                               traptor_notify_channel=traptor_notify_channel,
                               rule_check_interval=2,
                               traptor_type=request.param,
                               traptor_id=0,
                               apikeys=APIKEYS,
                               kafka_enabled=False,
                               kafka_hosts='localhost:9092',
                               kafka_topic='traptor_test',
                               use_sentry='False',
                               sentry_url=None,
                               log_level='DEBUG',
                               log_dir='logs',
                               log_file_name='traptor.log',
                               test=True
                               )

    return traptor_instance


@pytest.fixture
def tweets(request, traptor):
    """Create a list of tweets."""
    with open('tests/data/' + traptor.traptor_type + '_tweet.json') as f:
        loaded_tweet = json.load(f)

    return loaded_tweet

@pytest.fixture
def no_match_tweet(request, traptor):
    """Create a list of non-tweet messages."""
    with open('tests/data/no_match_tweet.json') as f:
        loaded_tweet = json.load(f)

    return loaded_tweet


@pytest.fixture
def non_tweet_stream_messages(request, traptor):
    """Create a list of non-tweet stream messages."""
    for message_file in os.listdir("tests/data/"):
        if message_file.endswith("_message.json"):
            with open("tests/data/" + message_file) as f:
                stream_messages = json.load(f)

    return stream_messages


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


@pytest.fixture(params=['classic.json', 'classic_hidden.json', 'extended_hidden.json'])
def extended_tweets(request):
    with open('tests/data/extended_tweets/{}'.format(request.param)) as f:
        loaded_tweet = json.load(f)

    return loaded_tweet


class TestRuleExtract():
    """Test the rule extraction functionality."""

    def test_track(self, redis_rules):
        """Test retrieving the tracking rules."""
        assert {'tag': 'test', 'value': 'happy', 'status': 'active', 'description': 'Tweets for a hashtag', 'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34', 'rule_type': 'track'} == redis_rules.hgetall('traptor-track:0:0')

    def test_follow(self, redis_rules):
        """Test retrieving the follow rules."""
        assert {'tag': 'test', 'value': '17919972', 'status': 'active', 'description': 'Tweets from some user', 'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34', 'rule_type': 'follow'} == redis_rules.hgetall('traptor-follow:0:0')

    def test_locations(self, redis_rules):
        """Test retrieving the location rules."""
        assert {'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8', 'status': 'active', 'description': 'Tweets from some continent', 'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34', 'rule_type': 'locations'} == redis_rules.hgetall('traptor-locations:0:0')


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

    def test_check_redis_pubsub_for_restart(self, traptor):
        """Test pubsub message causes the restart_flag to be set to True."""
        traptor._setup()
        # mock response
        old_logger = traptor.logger.debug
        data = {'data': traptor.traptor_type + ':0'}
        retobj = MagicMock()
        retobj.get_message = MagicMock(return_value=data)
        traptor.pubsub_conn = MagicMock()
        traptor.pubsub_conn.pubsub = MagicMock(return_value=retobj)
        self.counter = 0
        def fake(*args, **kwargs):
            self.counter += 1
            if self.counter <= 2:
                return None
            else:
                raise Exception('called')
        traptor.logger.debug = MagicMock(side_effect=fake)
        with pytest.raises(Exception) as ex:
            traptor._check_redis_pubsub_for_restart()
        assert ex.value.message == 'called'
        assert traptor.restart_flag == True
        traptor.logger.debug = old_logger
        assert True

    def test_redis_rules(self, redis_rules, traptor):
        """Ensure the correct rules are retrieved for the Traptor type."""
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]

        if traptor.traptor_type == 'track':
            assert traptor.redis_rules == [{'tag': 'test', 'value': 'happy', 'status': 'active', 'description': 'Tweets for a hashtag', 'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34', 'rule_type': 'track'}]
        if traptor.traptor_type == 'follow':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '17919972', 'status': 'active', 'description': 'Tweets from some user', 'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34', 'rule_type': 'follow'}]
        if traptor.traptor_type == 'locations':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8', 'status': 'active', 'description': 'Tweets from some continent', 'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34', 'rule_type': 'locations'}]

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

        if traptor.traptor_type == 'locations':
            traptor.locations_rule = traptor._get_locations_traptor_rule()

        # The birdy_stream will just look like whatever tweet has been loaded
        traptor.birdy_stream = MagicMock(return_value=tweets)
        traptor.birdy_stream.stream = traptor.birdy_stream

        tweet = traptor.birdy_stream.stream()

        # Do the rule matching against the redis rules
        enriched_data = traptor._enrich_tweet(tweet)

        # This is actually testing the rule matching
        if traptor.traptor_type == 'track':
            assert enriched_data['traptor']['created_at_iso'] == '2016-02-22T01:34:53+00:00'
            assert enriched_data['traptor']['rule_tag'] == 'test'
            assert enriched_data['traptor']['rule_value'] == 'happy'
            assert enriched_data['traptor']['rule_type'] == 'track'
            assert enriched_data['traptor']['tag'] == 'test'
            assert enriched_data['traptor']['value'] == 'happy'
            assert enriched_data['traptor']['status'] == 'active'
            assert enriched_data['traptor']['description'] == 'Tweets for a hashtag'
            assert enriched_data['traptor']['appid'] == 'test-appid'

        if traptor.traptor_type == 'follow':
            assert enriched_data['traptor']['created_at_iso'] == '2016-02-20T03:52:59+00:00'
            assert enriched_data['traptor']['rule_tag'] == 'test'
            assert enriched_data['traptor']['rule_value'] == '17919972'
            assert enriched_data['traptor']['rule_type'] == 'follow'
            assert enriched_data['traptor']['tag'] == 'test'
            assert enriched_data['traptor']['value'] == '17919972'
            assert enriched_data['traptor']['status'] == 'active'
            assert enriched_data['traptor']['description'] == 'Tweets from some user'
            assert enriched_data['traptor']['appid'] == 'test-appid'

        if traptor.traptor_type == 'locations':
            assert enriched_data['traptor']['created_at_iso'] == '2016-02-23T02:02:54+00:00'
            assert enriched_data['traptor']['rule_tag'] == 'test'
            assert enriched_data['traptor']['rule_value'] == '-122.75,36.8,-121.75,37.8'
            assert enriched_data['traptor']['rule_type'] == 'locations'
            assert enriched_data['traptor']['tag'] == 'test'
            assert enriched_data['traptor']['value'] == '-122.75,36.8,-121.75,37.8'
            assert enriched_data['traptor']['status'] == 'active'
            assert enriched_data['traptor']['description'] == 'Tweets from some continent'
            assert enriched_data['traptor']['appid'] == 'test-appid'

        """ Now test the with the new extended_tweet format """

        # # The birdy_stream will just look like whatever tweet has been loaded
        # traptor.birdy_stream = MagicMock(return_value=extended_tweets)
        # traptor.birdy_stream.stream = traptor.birdy_stream

        # # Test that we can set the tweet to the .stream() method
        # tweet = traptor.birdy_stream.stream()

        # # Do the rule matching against the redis rules
        # enriched_data = traptor._enrich_tweet(tweet)

        # if traptor.traptor_type == 'follow':
        #     assert enriched_data['traptor'] == x


    @pytest.mark.extended
    # @pytest.mark.parametrize('traptor', ['follow'])
    def test_main_loop_extended(self, traptor, extended_tweets):
        """Ensure we can loop through the streaming Twitter data."""
        traptor._setup()

        # The birdy_stream will just look like whatever tweet has been loaded
        traptor.birdy_stream = MagicMock(return_value=extended_tweets)
        traptor.birdy_stream.stream = traptor.birdy_stream

        tweet = traptor.birdy_stream.stream()

        if traptor.traptor_type == 'follow':
            with open('tests/data/extended_tweets/follow_rules.json') as f:
                traptor.redis_rules = [json.load(f)]
            traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

            # Do the rule matching against the redis rules
            enriched_data = traptor._enrich_tweet(tweet)

            assert enriched_data['traptor']['value'] == '735369652956766200'

        if traptor.traptor_type == 'track':
            with open('tests/data/extended_tweets/track_rules.json') as f:
                traptor.redis_rules = [json.load(f)]
            traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

            # Do the rule matching against the redis rules
            enriched_data = traptor._enrich_tweet(tweet)

            assert enriched_data['traptor']['value'] == 'tweet'

    def test_ensure_traptor_is_in_tweet_on_no_match(self, traptor, no_match_tweet):
        """Ensure that the traptor section is added to a tweet when no rule matches."""
        traptor._setup()
        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)
        traptor.birdy_stream = MagicMock(return_value=no_match_tweet)
        traptor.birdy_stream.stream = traptor.birdy_stream

        tweet = traptor.birdy_stream.stream()
        enriched_data = traptor._enrich_tweet(tweet)

        if traptor.traptor_type in ['track', 'follow', 'locations']:
            assert enriched_data['traptor']['created_at_iso'] == '2016-02-22T01:34:53+00:00'

        if traptor.traptor_type in ['track', 'follow']:
            assert enriched_data['traptor']['rule_tag'] == 'Not found'
            assert enriched_data['traptor']['rule_value'] == 'Not found'

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

    def test_ensure_traptor_only_enriches_tweets(self, traptor, non_tweet_stream_messages):
        """Ensure Traptor only performs rule matching on tweets."""
        traptor._setup()

        for message in non_tweet_stream_messages:
            print(message)
            enriched_data = traptor._enrich_tweet(message)
            assert enriched_data == message

    def test_ensure_traptor_stays_alive_until_rules_are_found(self, traptor):
        traptor._setup()
        traptor.rule_check_interval = 2
        traptor.logger.debug = MagicMock()

        empty_response = {}
        good_response = {"Rule": "Fun"}

        traptor._get_redis_rules = MagicMock(side_effect=[empty_response, empty_response, good_response])
        traptor._wait_for_rules()

        assert traptor.logger.debug.call_count == 2
