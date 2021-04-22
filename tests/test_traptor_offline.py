# coding=utf-8
"""Traptor unit tests."""
# To run with autotest and coverage and print all output to console run:
#   py.test -s --cov=traptor --looponfail tests/
from collections import deque
from datetime import datetime, timedelta
import time
import os
import json

import token_bucket
from redis import StrictRedis, ConnectionError
import pytest
from mock import MagicMock, call
import mockredis
from tenacity import wait_none

from traptor import version
from traptor.birdy.twitter import TwitterApiError, TwitterAuthError
from traptor.traptor import Traptor
from traptor.traptor_birdy import TraptorBirdyClient
from traptor.traptor_limit_counter import TraptorLimitCounter
from scripts.rule_extract import RulesToRedis
from scutils.log_factory import LogObject
from scutils.stats_collector import RollingTimeWindow


def getAppParamStr(aEnvVar, aDefault=None, aCliArg=None):
    """
    Retrieves a string parameter from either the environment
    var or CLI param that overrides it, using aDefault if
    neither are defined.

    :param str aEnvVar: the name of the Environment variable.
    :param str aDefault: the default value to use if None found.
    :param str aCliArg: the name of the CLI argument.
    :return: str: Returns the parameter value to use.
    """
    if aCliArg and aCliArg.strip():
        return aCliArg.strip()
    else:
        return os.getenv(aEnvVar, aDefault)


@pytest.fixture()
def redis_conn():
    redis_conn = mockredis.mock_strict_redis_client(host='localhost', port=6379, db=5)
    return redis_conn


@pytest.fixture()
def pubsub_conn():
    """Create a connection for the Redis PubSub."""
    p_conn = mockredis.mock_strict_redis_client(host='localhost', port=6379, db=5)
    return p_conn


@pytest.fixture()
def traptor_notify_channel():
    """Create a traptor notification channel."""
    return getAppParamStr(
        'REDIS_PUBSUB_CHANNEL', 'traptor-notify'
    )


@pytest.fixture()
def redis_rules(request, redis_conn):
    """Load up some sample traptor rules into Redis."""
    with open('tests/data/track_rules.json') as f:
        track_rules = [json.loads(line) for line in f]
    with open('tests/data/follow_rules.json') as f:
        follow_rules = [json.loads(line) for line in f]
    with open('tests/data/locations_rules.json') as f:
        locations_rules = [json.loads(line) for line in f]

    # conn = StrictRedis(host=HOST_FOR_TESTING, port=6379, db=5)
    conn = redis_conn
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
def mock_redis_info_success_response():
    """Mock instance of a successful redis.info response."""
    info = {
        'redis_version': '999.999.999',
        'redis_git_sha1': '1e09e31b11',
        'redis_git_dirty': 0,
        'redis_build_id': 'f05c0e7d7d91e005',
        'redis_mode': 'standalone',
        'os': 'Linux 4.8.0 - 1 - amd64 x86_64',
        'arch_bits': 64
    }

    return info


@pytest.fixture(params=['track',
                        'follow',
                        'locations',
                        ])
def traptor(request, redis_conn, pubsub_conn, heartbeat_conn, traptor_notify_channel, mock_redis_info_success_response):
    """Create a Traptor instance."""
    APIKEYS = ({
        'CONSUMER_KEY': '',
        'CONSUMER_SECRET': '',
        'ACCESS_TOKEN': '',
        'ACCESS_TOKEN_SECRET': ''
    })
    traptor_instance = Traptor(redis_conn=redis_conn,
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
                               use_sentry=False,
                               sentry_url=None,
                               test=True
                               )

    traptor_instance.redis_conn.info = MagicMock(
        side_effect=[mock_redis_info_success_response])

    traptor_instance.logger = MagicMock()

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
    stream_messages = list()

    for message_file in os.listdir("tests/data/"):
        if message_file.endswith("_message.json"):
            with open("tests/data/" + message_file) as f:
                stream_messages.append(json.load(f))

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
    hb_conn = mockredis.mock_strict_redis_client(host='localhost', port=6379, db=5)
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
        assert {'tag': 'test', 'value': 'happy', 'status': 'active', 'description': 'Tweets for a hashtag',
                'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34',
                'rule_type': 'track', 'orig_type': 'keyword', 'rule_id': '12347'} == redis_rules.hgetall('traptor-track:0:0')

    def test_follow(self, redis_rules):
        """Test retrieving the follow rules."""
        assert {'tag': 'test', 'value': '17919972', 'status': 'active', 'description': 'Tweets from some user',
                'appid': 'test-appid', 'date_added': '2016-05-10 16:58:34',
                'rule_type': 'follow', 'orig_type': 'userid', 'rule_id': '12345'} == redis_rules.hgetall('traptor-follow:0:0')

    def test_locations(self, redis_rules):
        """Test retrieving the location rules."""
        assert {'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8', 'status': 'active',
                'description': 'Tweets from some continent', 'appid': 'test-appid',
                'date_added': '2016-05-10 16:58:34',
                'rule_type': 'locations', 'orig_type': 'geo', 'rule_id': '12346'} == redis_rules.hgetall('traptor-locations:0:0')


class TestTraptor(object):
    """Traptor tests."""

    # Internal Functions
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
        assert traptor._getRestartSearchFlag() is False
        assert traptor.kafka_conn is None
        assert isinstance(traptor.birdy_conn, TraptorBirdyClient)

    def test_check_redis_pubsub_for_restart(self, traptor):
        """Test pubsub message causes the restart_flag to be set to True."""
        traptor._setup()
        # mock response
        data = {'data': traptor.traptor_type + ':' + str(traptor.traptor_id)}

        def stop_after(*args, **kwargs):
            traptor.exit = True
            return data

        pubsub = MagicMock()
        pubsub.subscribe = MagicMock()
        pubsub.get_message = MagicMock(side_effect=stop_after)
        pubsub.close = MagicMock()
        traptor.pubsub_conn = MagicMock()
        traptor.pubsub_conn.pubsub = MagicMock(return_value=pubsub)

        traptor._listenToRedisForRestartFlag()
        restart_flag = traptor._getRestartSearchFlag()
        assert restart_flag == True
        assert pubsub.subscribe.call_count == 1
        assert pubsub.get_message.call_count == 1
        assert pubsub.close.call_count == 1

    def test_redis_rules(self, redis_rules, traptor):
        """Ensure the correct rules are retrieved for the Traptor type."""
        traptor._setup()
        traptor.redis_conn = redis_rules

        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]

        if traptor.traptor_type == 'track':
            assert traptor.redis_rules == [{'tag': 'test', 'value': 'happy', 'status': 'active',
                                            'description': 'Tweets for a hashtag', 'appid': 'test-appid',
                                            'date_added': '2016-05-10 16:58:34', 'rule_type': 'track',
                                            'orig_type': 'keyword', 'rule_id': '12347'}]
        if traptor.traptor_type == 'follow':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '17919972', 'status': 'active',
                                            'description': 'Tweets from some user', 'appid': 'test-appid',
                                            'date_added': '2016-05-10 16:58:34', 'rule_type': 'follow',
                                            'orig_type': 'userid', 'rule_id': '12345'}]
        if traptor.traptor_type == 'locations':
            assert traptor.redis_rules == [{'tag': 'test', 'value': '-122.75,36.8,-121.75,37.8', 'status': 'active',
                                            'description': 'Tweets from some continent', 'appid': 'test-appid',
                                            'date_added': '2016-05-10 16:58:34', 'rule_type': 'locations',
                                            'orig_type': 'geo', 'rule_id': '12346'}]

    def test_twitter_rules(self, redis_rules, traptor):
        """Ensure Traptor can create Twitter rules from the Redis rules."""
        traptor._setup()
        traptor.redis_conn = redis_rules

        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

        if traptor.traptor_type == 'track':
            assert traptor.twitter_rules == 'happy'
        if traptor.traptor_type == 'follow':
            assert traptor.twitter_rules == '17919972'
        if traptor.traptor_type == 'locations':
            assert traptor.twitter_rules == '-122.75,36.8,-121.75,37.8'

    # Heartbeat and Stay Alive

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

    def test_ensure_traptor_stays_alive_until_rules_are_found(self, traptor):
        traptor._setup()

        traptor.rule_check_interval = 2
        traptor.logger.info = MagicMock()

        empty_response = {}
        good_response = {"Rule": "Fun"}

        traptor._get_redis_rules = MagicMock(side_effect=[empty_response, empty_response, good_response])
        traptor._wait_for_rules()

        assert traptor.logger.info.call_count == 3

    def test_ensure_traptor_builds_the_correct_filter_string(self, traptor):

        traptor.logger = MagicMock()
        traptor.traptor_type = 'track'

        traptor.twitter_rules = traptor._make_twitter_rules([
            {
                "rule_id": u"1",
                "value": u"happy",
                "rule_type": u"track",
                "orig_type": u"keyword"
            },
            {
                "rule_id": u"2",
                "value": u"summer",
                "rule_type": u"track",
                "orig_type": u"hashtag"
            },
            {
                "rule_id": u"3",
                "value": u"#apple",
                "rule_type": u"track",
                "orig_type": u"hashtag"
            },
            {
                "rule_id": u"4",
                "value": u"#sliding door",
                "rule_type": u"track",
                "orig_type": u"hashtag"
            },
            {
                "rule_id": u"5",
                "value": u"summer",
                "rule_type": u"track",
                "orig_type": u"hashtag"
            },
            {
                "rule_id": u"6",
                "value": u"今日中国",
                "rule_type": u"track",
                "orig_type": u"keyword"
            }])

        assert traptor.twitter_rules == u'#apple,#sliding #door,#summer,今日中国,happy'

    # Tweet Enrichments

    def test_ensure_traptor_only_enriches_tweets(self, traptor, non_tweet_stream_messages):
        """Ensure Traptor only performs rule matching on tweets."""
        traptor._setup()
        traptor.limit_counter = MagicMock()

        if traptor.traptor_type in ['track']:
            for message in non_tweet_stream_messages:
                enriched_data = traptor._enrich_tweet(message)
                assert enriched_data == message

    def test_ensure_traptor_is_in_tweet_on_no_match(self, redis_rules, traptor, no_match_tweet):
        """Ensure that the traptor section is added to a tweet when no rule matches."""
        traptor._setup()
        traptor.redis_conn = redis_rules

        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

        traptor.birdy_stream = MagicMock(return_value=no_match_tweet)
        traptor.birdy_stream.stream = traptor.birdy_stream

        tweet = traptor.birdy_stream.stream()
        enriched_data = traptor._enrich_tweet(tweet)

        if traptor.traptor_type in ['track', 'follow', 'locations']:
            assert enriched_data['traptor']['created_at_iso'] == '2016-02-22T01:34:53+00:00'

        if traptor.traptor_type in ['track', 'follow']:
            assert enriched_data['traptor']['rule_tag'] == 'Not Found'
            assert enriched_data['traptor']['rule_value'] == 'Not Found'

    def test_ensure_rule_counters_correctly_created(self, redis_rules, traptor):
        """Ensure _create_rule_counter correctly creates rule counters."""
        traptor._setup()
        traptor.redis_conn = redis_rules

        actual_counter = traptor._create_rule_counter('python')

        assert isinstance(actual_counter, RollingTimeWindow)

    def test_ensure_internal_rule_counters_are_correctly_made_for_rules(self, redis_rules, traptor):
        """Ensure _make_rule_counters makes the correct number of rule counters for the internal rules."""
        traptor._setup()
        traptor.redis_conn = redis_rules

        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

        if traptor.traptor_type != 'locations':
            traptor._make_rule_counters()

            assert len(traptor.rule_counters) == 1

        if traptor.traptor_type == 'track':
            assert traptor.rule_counters['12347'] is not None

        if traptor.traptor_type == 'follow':
            assert traptor.rule_counters['12345'] is not None

        if traptor.traptor_type == 'locations':
            assert len(traptor.rule_counters) == 0

    def test_ensure_internal_rule_counters_are_properly_deleted(self, redis_rules, traptor):
        """Ensure _delete_rule_counters are properly deleted."""
        traptor._setup()
        traptor.redis_conn = redis_rules

        traptor.logger.info = MagicMock()
        traptor.logger.error = MagicMock()

        traptor.redis_rules = [rule for rule in traptor._get_redis_rules()]
        traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

        if traptor.traptor_type != 'locations':
            traptor._make_rule_counters()
            assert len(traptor.rule_counters) == 1

        if traptor.traptor_type == 'track':
            assert traptor.rule_counters['12347'] is not None

            traptor._delete_rule_counters()
            assert traptor.logger.error.call_count == 0
            assert traptor.logger.info.call_count == 3

        if traptor.traptor_type == 'follow':
            assert traptor.rule_counters['12345'] is not None

            traptor._delete_rule_counters()
            assert traptor.logger.error.call_count == 0
            assert traptor.logger.info.call_count == 3

        if traptor.traptor_type == 'locations':
            assert len(traptor.rule_counters) == 0

    def test_ensure_limit_message_counter_is_correctly_created(self, redis_rules, traptor):
        """Ensure _make_limit_message_counter makes the limit counter"""
        traptor._setup()
        traptor.redis_conn = redis_rules

        if traptor.traptor_type != 'locations':
            traptor._make_limit_message_counter()
            assert traptor.limit_counter is not None
            assert type(traptor.limit_counter) == TraptorLimitCounter

            l_key = "limit:{}:{}".format(traptor.traptor_type, traptor.traptor_id)
            assert traptor.limit_counter.get_key() == l_key

    # Main Loop

    def test_main_loop(self, redis_rules, traptor, tweets):
        """Ensure we can loop through the streaming Twitter data."""
        traptor._setup()

        traptor.redis_conn = redis_rules

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
            assert enriched_data['traptor']['collection_rules'][0]['rule_tag'] == 'test'
            assert enriched_data['traptor']['collection_rules'][0]['rule_value'] == 'happy'
            assert enriched_data['traptor']['collection_rules'][0]['rule_type'] == 'track'
            assert enriched_data['traptor']['collection_rules'][0]['tag'] == 'test'
            assert enriched_data['traptor']['collection_rules'][0]['value'] == 'happy'
            assert enriched_data['traptor']['collection_rules'][0]['status'] == 'active'
            assert enriched_data['traptor']['collection_rules'][0]['description'] == 'Tweets for a hashtag'
            assert enriched_data['traptor']['collection_rules'][0]['appid'] == 'test-appid'

        if traptor.traptor_type == 'follow':
            assert enriched_data['traptor']['created_at_iso'] == '2016-02-20T03:52:59+00:00'
            assert enriched_data['traptor']['collection_rules'][0]['rule_tag'] == 'test'
            assert enriched_data['traptor']['collection_rules'][0]['rule_value'] == '17919972'
            assert enriched_data['traptor']['collection_rules'][0]['rule_type'] == 'follow'
            assert enriched_data['traptor']['collection_rules'][0]['tag'] == 'test'
            assert enriched_data['traptor']['collection_rules'][0]['value'] == '17919972'
            assert enriched_data['traptor']['collection_rules'][0]['status'] == 'active'
            assert enriched_data['traptor']['collection_rules'][0]['description'] == 'Tweets from some user'
            assert enriched_data['traptor']['collection_rules'][0]['appid'] == 'test-appid'

        if traptor.traptor_type == 'locations':
            assert enriched_data['traptor']['created_at_iso'] == '2016-02-23T02:02:54+00:00'
            assert enriched_data['traptor']['collection_rules'][0]['rule_tag'] == 'test'
            assert enriched_data['traptor']['collection_rules'][0]['rule_value'] == '-122.75,36.8,-121.75,37.8'
            assert enriched_data['traptor']['collection_rules'][0]['rule_type'] == 'locations'
            assert enriched_data['traptor']['collection_rules'][0]['tag'] == 'test'
            assert enriched_data['traptor']['collection_rules'][0]['value'] == '-122.75,36.8,-121.75,37.8'
            assert enriched_data['traptor']['collection_rules'][0]['status'] == 'active'
            assert enriched_data['traptor']['collection_rules'][0]['description'] == 'Tweets from some continent'
            assert enriched_data['traptor']['collection_rules'][0]['appid'] == 'test-appid'

    @pytest.mark.extended
    def test_main_loop_extended(self, redis_rules, traptor, extended_tweets):
        """Ensure we can loop through the streaming Twitter data."""
        traptor._setup()
        traptor.redis_conn = redis_rules

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

            assert enriched_data['traptor']['collection_rules'][0]['rule_value'] == '735369652956766200'

        if traptor.traptor_type == 'track':
            with open('tests/data/extended_tweets/track_rules.json') as f:
                traptor.redis_rules = [json.load(f)]
            traptor.twitter_rules = traptor._make_twitter_rules(traptor.redis_rules)

            # Do the rule matching against the redis rules
            enriched_data = traptor._enrich_tweet(tweet)

            assert enriched_data['traptor']['collection_rules'][0]['rule_value'] == 'tweet'

    def test_invalid_follow_returns_blank(self, traptor):
        traptor.logger = MagicMock()
        traptor.logger = MagicMock()
        traptor.logger = MagicMock()
        traptor.traptor_type = 'follow'
        rules_str = traptor._make_twitter_rules([{"value": "ishouldbeint"}])

        assert len(rules_str) == 0

    def test_connection_retry(self, traptor):
        """Ensure we can correctly retry connection failures, throttling, and auth failures."""

        traptor.logger = MagicMock()

        # Trade out the wait policy for the unit test
        traptor._create_birdy_stream.retry.wait = wait_none()

        if traptor.traptor_type == 'follow':
            traptor._create_twitter_follow_stream = MagicMock(side_effect=None)

            try:
                traptor._create_birdy_stream()
            except Exception as e:
                # Follow should not throw exception
                raise e

            assert traptor._create_twitter_follow_stream.call_count == 1

        elif traptor.traptor_type == 'track':
            traptor._create_twitter_track_stream = MagicMock(side_effect=[
                TwitterApiError("api error 1"),
                TwitterApiError("api error 2"),
                TwitterApiError("api error 3"),
                TwitterApiError("api error 4"),
                None  # Finally succeed
            ])

            try:
                traptor._create_birdy_stream()
            except Exception as e:
                # Track should not throw exception
                raise e

            assert traptor._create_twitter_track_stream.call_count == 5

        elif traptor.traptor_type == 'locations':
            traptor._create_twitter_locations_stream = MagicMock(side_effect=TwitterAuthError("auth error"))

            try:
                traptor._create_birdy_stream()
            except TwitterAuthError as e:
                # Locations should throw TwitterAuthError
                pass

            assert traptor._create_twitter_locations_stream.call_count == 1

    def test_token_bucket(self):
        storage = token_bucket.MemoryStorage()
        limiter = token_bucket.Limiter(10, 10, storage)
        for i in range(30):
            if limiter.consume('key'):
                print("Write to kafka")
            else:
                print("Filter")
            import time
            time.sleep(.05)

    def test_rate_logger(self, traptor):
        traptor.kafka_rate['test'] = deque([1, 2, 3])
        traptor.twitter_rate['test'] = deque([1, 2, 3])
        traptor.logger = MagicMock()
        traptor._log_rates(4.0, 4.0)
        assert traptor.logger.method_calls ==[call.info('Twitter Rate', extra={'count': 3, 'tags': ['traptor_type:None', 'traptor_id:None'], 'rule_value': 'test', 'max_tps': 1.0, 'component': 'traptor', 'min_tps': 0.0, 'average_tps': 0.75, 'traptor_version': version.__version__}),
                                              call.info('Kafka Rate', extra={'count': 3, 'tags': ['traptor_type:None', 'traptor_id:None'], 'rule_value': 'test', 'max_tps': 1.0, 'component': 'traptor', 'min_tps': 0.0, 'average_tps': 0.75, 'traptor_version': version.__version__})]

    def test_rate_logger_out_of_range(self, traptor):
        traptor.kafka_rate['test'] = deque()
        traptor.twitter_rate['test'] = deque()
        traptor.logger = MagicMock()
        traptor._log_rates(4.0, 4.0)
        assert traptor.logger.method_calls == [call.info('Twitter Rate', extra={'count': 0, 'tags': ['traptor_type:None', 'traptor_id:None'], 'rule_value': 'test', 'max_tps': 0.0, 'component': 'traptor', 'min_tps': 0.0, 'average_tps': 0.0, 'traptor_version': version.__version__}),
                                               call.info('Kafka Rate', extra={'count': 0, 'tags': ['traptor_type:None', 'traptor_id:None'], 'rule_value': 'test', 'max_tps': 0.0, 'component': 'traptor', 'min_tps': 0.0, 'average_tps': 0.0, 'traptor_version': version.__version__})]

    def test_rate_logger_length_one(self, traptor):
        traptor.kafka_rate['test'] = deque([2])
        traptor.twitter_rate['test'] = deque([2])
        traptor.logger = MagicMock()
        traptor._log_rates(4.0, 4.0)
        assert traptor.logger.method_calls == [call.info('Twitter Rate', extra={'count': 1, 'tags': ['traptor_type:None', 'traptor_id:None'], 'rule_value': 'test', 'max_tps': 1.0, 'component': 'traptor', 'min_tps': 0.0, 'average_tps': 0.25, 'traptor_version': version.__version__}),
                                               call.info('Kafka Rate', extra={'count': 1, 'tags': ['traptor_type:None', 'traptor_id:None'], 'rule_value': 'test', 'max_tps': 1.0, 'component': 'traptor', 'min_tps': 0.0, 'average_tps': 0.25, 'traptor_version': version.__version__})]

    def test_filter_maintenance(self, traptor):
        now = time.time()
        traptor.twitter_rate['test'] = deque([time.time() - 360, time.time() - 240, time.time() - 120, now])
        traptor.kafka_rate['test'] = deque([time.time() - 360, time.time() - 240, time.time() - 120, now])

        traptor._filter_maintenance(now, 30.0)

        assert traptor.twitter_rate['test'] == deque([now])
        assert traptor.kafka_rate['test'] == deque([now])
        traptor._filter_maintenance(now, 30.0)

    def test_is_filtered_one_rule_value(self, traptor):

        enriched_data = {
            "traptor": {
                "collection_rules": [{"value": "air force"}]
            }
        }

        traptor.rate_limiting_enabled = True

        for i in range(100):
            traptor._is_filtered(enriched_data)
            time.sleep(.01)

        assert len(traptor.twitter_rate['air force']) == 100
        assert 20 <= len(traptor.kafka_rate['air force']) <= 22

    def test_is_filtered_dummy(self, traptor):
        enriched_data = {
            "traptor": {
                "collection_rules": [{"value": "air force"}]
            }
        }

        twitter_rate = dict()
        kafka_rate = dict()
        rate_limiter = dict()
        rule_last_seen = dict()

        key = enriched_data['traptor']['collection_rules'][0]['value']
        rule_last_seen[key] = datetime.now()

        traptor.logger = MagicMock()
        # check only if received tweet
        if key in rule_last_seen:
            value = rule_last_seen[key]
            upper_bound = datetime.now()
            lower_bound = upper_bound - timedelta(minutes=2)

            # thread interval every 2 minutes or function in traptor every 10,000 tweets
            # function filter_maintance


            for key, value in rule_last_seen.items():
                if upper_bound >= value >= lower_bound:
                    print('Last seen less than 2 minutes ago')
                    print(rule_last_seen)
                else:
                    print('Last seen longer than 2 minutes ago, drop it')  # debug
                    del rule_last_seen[key], twitter_rate[key], kafka_rate[key], rate_limiter[key]
                    print(rule_last_seen)
        else:
            rule_last_seen[key] = datetime.now()

        if key not in rate_limiter:
            storage = token_bucket.MemoryStorage()
            limiter = token_bucket.Limiter(10, 10, storage)
            rate_limiter[key] = limiter
            twitter_rate[key] = "Twitter Rate"  # every tweet
            kafka_rate[key] = "Kafka Rate"  # wont know until after consume, only ones not filtered will be record here
            rule_last_seen[key] = datetime.now()

        # How to get rates , track yourself
        # rate_limiter[key].consume() # check boolean result, if false then filter.

        # create key, first timestamp, create new token bucket
        # True if token bucket
        for i in range(100):
            traptor._is_filtered(enriched_data)
            # number of trues match behavior expected

