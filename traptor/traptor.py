#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import json
import math
import signal
import sys
import os
import time
import random
from collections import deque
from datetime import datetime, timedelta
# noinspection PyPackageRequirements
import dateutil.parser as parser
import traceback
import threading

import redis
import token_bucket

import dd_monitoring
import six

# noinspection PyPackageRequirements
from kafka import KafkaProducer
# noinspection PyPackageRequirements
from kafka.common import KafkaUnavailableError

from birdy.twitter import TwitterApiError, TwitterAuthError, TwitterRateLimitError, BirdyException

from tenacity import retry, wait_exponential, stop_after_attempt, \
    retry_if_exception_type, wait_chain, wait_fixed, wait_incrementing, stop_never, retry_if_exception

from dog_whistle import dw_config, dw_callback
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from scutils.log_factory import LogFactory
from scutils.stats_collector import StatsCollector

from rule_set import RuleSet
from traptor_birdy import TraptorBirdyClient
from traptor_limit_counter import TraptorLimitCounter

import settings
import argparse
import version
import types

# Vars initialized once, then threadsafe to use
my_component = 'traptor'
my_traptor_type = None
my_traptor_id = None
my_logger = None


class retry_if_exception_type_with_caveat(retry_if_exception):
    """Retries if an exception has been raised of one or more types."""

    def __init__(self, exception_types=(Exception,), excluding=None):
        self.exception_types = exception_types
        self.excluding_types = excluding

        super(retry_if_exception_type_with_caveat, self).__init__(
            lambda e: isinstance(e, exception_types) and not isinstance(e, excluding)
        )


# Thanks to https://stackoverflow.com/a/715468
def str2bool(v):
    """
    Convert a string to a boolean

    :return boolean: Returns True if string is a true-type string.
    """
    return v.lower() in ('true', 't', '1', 'yes', 'y')


# Thanks to https://stackoverflow.com/a/26853961
def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    Backwards compatible function; Python 3.5+ equivalent of
    foo = {**x, **y, **z}
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def get_main():
    """
    What is our main app called? Not easy to find out.
    See https://stackoverflow.com/a/35514032

    :return: str: Returns the main filename, path excluded.
    """
    whatisrunning = sys.argv[0]
    if whatisrunning == '-c':
        whatisrunning = '{inline}'
    elif not whatisrunning:
        whatisrunning = my_component
    return os.path.basename(whatisrunning)


def logExtra(*info_args):
    """
    Generate standardized logging information.
    Arguments can be of types dict|str|Exception.

    :return: dict: Returns the `extra` param for logger.
    """
    result = {
            'component': my_component,
            my_component+'_version': version.__version__,
            'tags': ["traptor_type:{}".format(my_traptor_type),
                     "traptor_id:{}".format(my_traptor_id)]
    }
    for info in info_args:
        if isinstance(info, types.StringType):
            result = merge_dicts(result, {'dbg-info': info})
        elif isinstance(info, dict):
            result = merge_dicts(result, info)
        elif isinstance(info, Exception):
            result = merge_dicts(result, {
                    'error_type': info.__class__.__name__,
                    'error_msg': info.message,
                    'ex': traceback.format_exc(1),
            })
    return result


def log_retry_twitter(func, aRetryNum, arg3):
    """
    If a retry occurs, log it.

    :param func: this function reference.
    :param aRetryNum: the retry number.
    :param arg3: unknown decimal value, maybe time since last retry?
    """
    global my_logger
    if my_logger is not None:
        my_logger.info(settings.DWC_RETRY_TWITTER, extra=logExtra({
                'retry-num': aRetryNum,
        }))


def log_retry_redis(func, aRetryNum, arg3):
    """
    If a retry occurs, log it.

    :param func: this function reference.
    :param aRetryNum: the retry number.
    :param arg3: unknown decimal value, maybe time since last retry?
    """
    global my_logger
    if my_logger is not None:
        my_logger.info(settings.DWC_RETRY_REDIS, extra=logExtra({
                'retry-num': aRetryNum,
        }))


def log_retry_kafka(func, aRetryNum, arg3):
    """
    If a retry occurs, log it.

    :param func: this function reference.
    :param aRetryNum: the retry number.
    :param arg3: unknown decimal value, maybe time since last retry?
    """
    global my_logger
    if my_logger is not None:
        my_logger.info(settings.DWC_RETRY_KAFKA, extra=logExtra({
                'retry-num': aRetryNum,
        }))


class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class Traptor(object):

    def __init__(
            self,
            redis_conn,
            pubsub_conn,
            heartbeat_conn,
            traptor_notify_channel='traptor-notify',
            rule_check_interval=60,
            traptor_type='track',
            traptor_id=0,
            apikeys=None,
            kafka_enabled=True,
            kafka_hosts='localhost:9092',
            kafka_topic='traptor',
            use_sentry=False,
            sentry_url=None,
            test=False,
            enable_stats_collection=True,
            heartbeat_interval=0,
            rate_limiting_enabled=False,
            rate_limiting_rate_sec=10.0,
            rate_limiting_capacity=10,
            rate_limiting_reporting_interval_sec=60.0
            ):
        """
        Traptor base class.

        :param Redis redis_conn: redis connection to use.
        :param Redis pubsub_conn: redis pubsub connection to use.
        :param Redis heartbeat_conn: redis connection to use for the heartbeat
                messages.
        :param str traptor_notify_channel: name of the traptor PubSub channel
                to subscribe to.
        :param int rule_check_interval: number of seconds between checks of
                Redis for rules assigned to this traptor.
        :param str traptor_type: follow, track, or geo.
        :param int traptor_id: numerical ID of traptor instance.
        :param dict apikeys: dictionary of API keys for traptor instnace.  See
                settings.py for details.
        :param bool kafka_enabled: write to kafka or just log to something else.
        :param str kafka_hosts: kafka hosts to connect to.
        :param str kafka_topic: name of the kafka topic to write to.
        :param bool use_sentry: use Sentry for error reporting or not.
        :param str sentry_url: url for Sentry logging.
        :param bool test: True for traptor test instance.
        :param bool enable_stats_collection: Whether or not to allow redis
                stats collection
        :param int heartbeat_interval: the number of seconds between heartbeats,
                if 0, a minimum value will be used instead.
        """

        self.redis_conn = redis_conn
        self.pubsub_conn = pubsub_conn
        self.heartbeat_conn = heartbeat_conn
        self.traptor_notify_channel = traptor_notify_channel
        self.rule_check_interval = rule_check_interval
        self.traptor_type = traptor_type
        self.traptor_id = traptor_id
        self.apikeys = apikeys
        self.kafka_enabled = kafka_enabled
        self.kafka_hosts = kafka_hosts
        self.kafka_topic = kafka_topic
        self.rate_limiting_enabled = rate_limiting_enabled
        self.rate_limiting_rate_sec = max(1.0, float(rate_limiting_rate_sec))
        self.rate_limiting_capacity = max(1, int(rate_limiting_capacity))
        self.rate_limiting_reporting_interval_sec = max(1.0, float(rate_limiting_reporting_interval_sec))
        self.use_sentry = use_sentry
        self.sentry_url = sentry_url
        self.test = test
        self.enable_stats_collection = enable_stats_collection
        self.accepting_assignments = True

        if heartbeat_interval < 5:
            heartbeat_interval = 5

        self.kafka_success_callback = self._gen_kafka_success()
        self.kafka_failure_callback = self._gen_kafka_failure()

        self.rule_counters = dict()
        self.limit_counter = None
        self.twitter_rules = None
        self.locations_rule = {}
        self.name = 'traptor-{}-{}'.format(
                self.traptor_type, self.traptor_id
        )
        # Properties that need to be accessed using a semaphore
        self._NEEDS_SEMAPHORE_hb_interval = heartbeat_interval
        self._NEEDS_SEMAPHORE_restart_flag = False
        # Semaphore used to make accessing properties thread-safe
        self.my_rlock = threading.RLock()

        self.rule_wait_event = threading.Event()
        self.exit_event = threading.Event()
        self.exit = False

        # three in memory dict that contain keys of rule values  (up to 400 rule values, removing keys or memory leak)
        # Map of rule value -> list of timestamps
        self.twitter_rate = dict()
        # Map of rule value -> list of kafka rate
        self.kafka_rate = dict()
        # Map of rule value -> token bucket
        self.rate_limiter = dict()

        self._last_filter_maintenance = -1

        def sigterm_handler(_signo, _stack_frame):
            self._exit()

        # Watch for SIGTERM, which is sent by Docker for clean a exit
        signal.signal(signal.SIGTERM, sigterm_handler)

    def _exit(self):
        """
        Sets the exit flag to True and signals the main thread to wake if it is
        sleeping.
        """
        self.logger.info("Traptor is set to exit its main event loop.")

        self.exit = True
        self.exit_event.set()

        # If we're currently waiting on rule assignment, wake up
        self.rule_wait_event.set()

    def __repr__(self):
        return 'Traptor(' \
               + 'type='+repr(self.traptor_type) \
               + ', id='+repr(self.traptor_id) \
               + ', heartbeat='+repr(self._hb_interval()) \
               + ', notify_channel='+repr(self.traptor_notify_channel) \
               + ', check_interval='+repr(self.rule_check_interval) \
               + ', apikeys='+repr(self.apikeys) \
               + ', kafka_on='+repr(self.kafka_enabled) \
               + ', ktopic='+repr(self.kafka_topic) \
               + ', sentry_on='+repr(self.use_sentry) \
               + ', test_on='+repr(self.test) \
               + ', stats_on='+repr(self.enable_stats_collection) \
               + ')'

    def _getRestartSearchFlag(self):
        """
        Thread-safe method to get the restart search flag value.

        :return: Return the restart flag value.
        """
        with self.my_rlock:
            return self._NEEDS_SEMAPHORE_restart_flag

    def _setRestartSearchFlag(self, aValue):
        """
        Thread-safe method to set the restart search flag value.

        :param bool aValue: the value to use.
        """
        theLogMsg = "Setting restart search flag"
        # logger is already thread-safe, no need to use semaphore around it
        self.logger.debug(theLogMsg, extra=logExtra(str(aValue)))
        with self.my_rlock:
            self._NEEDS_SEMAPHORE_restart_flag = aValue
            self.rule_wait_event.set()

    def _hb_interval(self, interval=None):
        """
        Thread-safe method to get/set the heartbeat value.
        Purposely combined getter/setter as possible alternative code style.

        :param number interval: the value to use.
        :return: Returns the value if interval param is not provided.
        """
        with self.my_rlock:
            if interval is None:
                return self._NEEDS_SEMAPHORE_hb_interval
            else:
                self._NEEDS_SEMAPHORE_hb_interval = interval

    def _setup_birdy(self):
        """ Set up a birdy twitter stream.
            If there is a TwitterApiError it will exit with status code 3.
            This was done to prevent services like supervisor from automatically
            restart the process causing the twitter API to get locked out.

            Creates ``self.birdy_conn``.
        """

        # Set up a birdy twitter streaming client
        self.logger.info('Setting up birdy client')
        self.birdy_conn = TraptorBirdyClient(
                self.apikeys['CONSUMER_KEY'],
                self.apikeys['CONSUMER_SECRET'],
                self.apikeys['ACCESS_TOKEN'],
                self.apikeys['ACCESS_TOKEN_SECRET']
        )

    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(KafkaUnavailableError),
        reraise = True,
        after=log_retry_kafka,
    )
    def _create_kafka_producer(self):
        """Create the Kafka producer"""
        self.kafka_conn = KafkaProducer(
                bootstrap_servers=self.kafka_hosts,
                value_serializer=lambda m: json.dumps(m),
                api_version=(0, 9),
                reconnect_backoff_ms=4000,
                retries=3,
                linger_ms=25,
                buffer_memory=4 * 1024 * 1024
        )

    def _setup_kafka(self):
        """ Set up a Kafka connection."""
        if self.kafka_enabled:
            self.logger.info('Setting up kafka connection')
            try:
                self._create_kafka_producer()
            except Exception as e:
                theLogMsg = "Caught Kafka Unavailable Error"
                self.logger.critical(theLogMsg, extra=logExtra({'value_str': str(self.kafka_hosts)}, e))
                dd_monitoring.increment('kafka_error',
                                        tags=['error_type:kafka_unavailable'])
                # sys.exit(3)
        else:
            self.logger.info('Skipping kafka connection setup')
            self.logger.debug('kafka_enabled', extra=logExtra(
                    str(self.kafka_enabled)
            ))
            self.kafka_conn = None

    def _gen_kafka_success(self):
        def kafka_success(tweet, response):
            self.logger.info("Tweet sent to kafka", extra=logExtra({
                'tweet_id': tweet.get('id_str', None)
            }))
            dd_monitoring.increment('tweet_to_kafka_success')
        return kafka_success

    def _gen_kafka_failure(self):
        def kafka_failure(e):
            theLogMsg = "Caught Kafka exception when sending a tweet to Kafka"
            self.logger.error(theLogMsg, extra=logExtra(e))
            dd_monitoring.increment('tweet_to_kafka_failure',
                                    tags=['error_type:kafka'])
        return kafka_failure

    def _setup(self, args=None, aLogger=None):
        """
        Set up Traptor.

        Load everything up. Note that any arg here will override both
        default and custom settings.

        :param args: CLI arguments, if any.
        :param aLogger: logger object, if any.
        """
        if args is None:
            args = dotdict({'log_stdout': '', 'loglevel': '', 'log_file': ''})
        # Set up logging
        self.logger = aLogger
        if aLogger is None:
            self.logger = LogFactory.get_instance(
                    name=self.name,
                    json=str2bool(os.getenv('LOG_JSON', settings.LOG_JSON)),
                    stdout=str2bool(getAppParamStr(
                        'LOG_STDOUT', settings.LOG_STDOUT, args.log_stdout
                    )),
                    level=getLoggingLevel(args.loglevel),
                    dir=os.getenv('LOG_DIR', settings.LOG_DIR),
                    file=getAppParamStr('LOG_FILE', settings.LOG_FILE, args.log_file)
            )
            if settings.DW_ENABLED:
                dw_config(settings.DW_CONFIG)
                self.logger.register_callback('>=INFO', dw_callback)

        # Set up required connections
        self._setup_kafka()
        self._setup_birdy()

    def _create_twitter_follow_stream(self):
        """Create a Twitter follow stream."""
        self.logger.info('Creating birdy follow stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(
                follow=self.twitter_rules,
                stall_warnings='true'
        )

    def _create_twitter_track_stream(self):
        """Create a Twitter follow stream."""
        self.logger.info('Creating birdy track stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(
                track=self.twitter_rules,
                stall_warnings='true'
        )

    def _create_twitter_locations_stream(self):
        """Create a Twitter locations stream."""
        self.logger.info('Creating birdy locations stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(
                locations=self.twitter_rules,
                stall_warnings='true'
        )

    @retry(
        wait=wait_incrementing(60, 60, 300),
        stop=stop_never,
        retry=retry_if_exception_type_with_caveat(BirdyException, excluding=TwitterAuthError),
        reraise=True,
        after=log_retry_twitter,
    )
    def _create_birdy_stream(self):
        """ Create a birdy twitter stream.
            If there is a TwitterApiError it will exit with status code 3.
            This was done to prevent services like supervisor from automatically
            restart the process causing the twitter API to get locked out.

            Creates ``self.birdy_stream``.
        """
        try:
            if self.traptor_type == 'follow':
                # Try to set up a twitter stream using twitter id list
                self._create_twitter_follow_stream()
            elif self.traptor_type == 'track':
                # Try to set up a twitter stream using twitter term list
                self._create_twitter_track_stream()
            elif self.traptor_type == 'locations':
                # Try to set up a twitter stream using twitter term list
                self._create_twitter_locations_stream()
            else:
                theLogMsg = 'Caught error creating birdy stream for Traptor ' \
                            'type that does not exist'
                self.logger.critical(theLogMsg, extra=logExtra({
                        'error_type': 'NotImplementedError',
                        'error_msg': 'Traptor type does not exist.',
                        'ex': traceback.format_exc(1)
                }))
                dd_monitoring.increment('traptor_error_occurred',
                                        tags=['error_type:not_implemented_error'])
        except Exception as e:
            theLogMsg = "Caught Twitter Api Error creating {} stream".format(self.traptor_type)
            self.logger.error(theLogMsg, extra=logExtra(e))
            dd_monitoring.increment('twitter_error_occurred',
                                    tags=['error_type:twitter_api_error'])
            raise

    def _make_twitter_rules(self, rules):
        """
        Convert the rules from redis into a format compatible with the
        Twitter API.

        This uses the RuleSet data structure, lifted from the Traptor Rule
        Manager, to ensure the resulting filter phrase list is consistent with
        the intent of the rule manager assignment and rule value de-duplication
        is properly handled.

        :param list rules: The rules are expected to be a list of
                            dictionaries that comes from redis.
        :returns: A ``str`` of twitter rules that can be loaded into the
                  a birdy twitter stream.
        """
        rule_set = RuleSet()

        for rule in rules:
            # Enforce some constraints
            if self.traptor_type == 'track':
                # The rule value is already unicode encoded at this point
                if len(rule['value']) <= 60:
                    rule_set.append(rule)
                else:
                    self.logger.error("Skipping invalid track rule, over 60 bytes", extra=logExtra({"value_str": json.dumps(rule, indent=4)}))

            elif self.traptor_type == 'follow':
                if str(rule['value']).isdigit():
                    rule_set.append(rule)
                else:
                    self.logger.error("Skipping invalid follow rule, not numeric", extra=logExtra({"value_str": json.dumps(rule, indent=4)}))
            else:
                rule_set.append(rule)

        phrases = u','.join(six.iterkeys(rule_set.rules_by_value))

        self.logger.debug('Twitter rules string: {}'.format(phrases.encode('utf-8')))

        return phrases

    def _create_rule_counter(self, rule_id):
        """
        Create a rule counter

        :param rule_id: id of the rule to create a counter for
        :return: stats_collector: StatsCollector rolling time window
        """
        collection_window = int(os.getenv('STATS_COLLECTION_WINDOW', 900))
        stats_key = 'stats:{}:{}:{}'.format(self.traptor_type, self.traptor_id, rule_id)
        stats_collector = StatsCollector.get_rolling_time_window(
                redis_conn=self.redis_conn,
                key=stats_key,
                window=collection_window
        )

        return stats_collector

    def _make_rule_counters(self):
        """
        Make the rule counters to collect stats on the rule matches.

        :return: dict: rule_counters
        """
        self.logger.info("Making the rule counters")

        rule_counters = dict()

        for rule in self.redis_rules:
            rule_id = rule['rule_id']
            rule_counters[rule_id] = self._create_rule_counter(rule_id=rule_id)

        self.rule_counters = rule_counters

    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
        retry=retry_if_exception_type(redis.ConnectionError),
        after=log_retry_redis,
    )
    def _increment_rule_counter(self, tweet):
        """
        Increment a rule counter.

        :param tweet: the tweet rule
        """
        collection_rules = tweet['traptor']['collection_rules']

        for rule in collection_rules:
            rule_id = rule.get('rule_id', None)

            # If the counter doesn't yet exist, create it
            if self.rule_counters.get(rule_id, None) is None:
                self.rule_counters[rule_id] = self._create_rule_counter(rule_id=rule_id)

            # If a rule value exists, increment the counter
            try:
                if rule_id is not None and self.rule_counters[rule_id] is not None:
                    self.rule_counters[rule_id].increment()
            except Exception as e:
                theLogMsg = "Caught exception while incrementing a rule counter"
                self.logger.error(theLogMsg, extra=logExtra(e))
                dd_monitoring.increment('redis_error',
                                        tags=['error_type:connection_error'])

    def _delete_rule_counters(self):
        """
        Stop and then delete the existing rule counters.
        """
        if len(self.rule_counters) > 0:
            for counter in self.rule_counters:
                try:
                    self.rule_counters[counter].deactivate()
                except Exception as e:
                    theLogMsg = "Caught exception while deactivating a rule counter"
                    self.logger.error(theLogMsg, extra=logExtra(e))
                    dd_monitoring.increment('redis_error',
                                            tags=['error_type:connection_error'])
            for counter in self.rule_counters:
                try:
                    self.rule_counters[counter].stop()
                    self.rule_counters[counter].delete_key()
                except Exception as e:
                    theLogMsg = "Caught exception while stopping and deleting a rule counter"
                    self.logger.error(theLogMsg, extra=logExtra(e))
                    dd_monitoring.increment('redis_error',
                                            tags=['error_type:connection_error'])
            self.logger.info("Rule counters deleted successfully", extra=logExtra())

    def _make_limit_message_counter(self):
        """
        Make a limit message counter to track the values of incoming limit messages.
        """
        limit_counter_key = "limit:{}:{}".format(
                self.traptor_type, self.traptor_id
        )
        collection_window = int(os.getenv('LIMIT_COUNT_COLLECTION_WINDOW', 900))

        self.limit_counter = TraptorLimitCounter(
                key=limit_counter_key,
                window=collection_window
        )
        self.limit_counter.setup(redis_conn=self.redis_conn)

    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
        retry=retry_if_exception_type(redis.ConnectionError),
        after=log_retry_redis,
    )
    def _increment_limit_message_counter(self, limit_count):
        """
        Increment the limit message counter

        :param limit_count: the integer value from the limit message
        """
        try:
            if self.limit_counter is not None:
                self.limit_counter.increment(limit_count=limit_count)
        except Exception as e:
            theLogMsg = "Caught exception while incrementing a limit counter"
            self.logger.error(theLogMsg, extra=logExtra(e))
            dd_monitoring.increment('redis_error',
                                    tags=['error_type:connection_error'])

    def _get_locations_traptor_rule(self):
        """
        Get the locations rule.

        Create a dict with the single rule the locations traptor collects on.
        """

        locations_rule = {}

        for rule in self.redis_rules:
            locations_rule['rule_tag'] = rule['tag']
            locations_rule['rule_value'] = rule['value']

            for key, value in rule.iteritems():
                locations_rule[key] = value

        return locations_rule

    def _get_url_fields(self, url_entities, collection):
        """

        :param url_entities:
        :type url_entities: list
        :param collection:
        :type collection: set
        """

        for item in url_entities:
            value = item.get('expanded_url')

            if value is not None:
                collection.add(value)

            value = item.get('display_url')

            if value is not None:
                collection.add(value)

    def _build_match_content(self, tweet):
        """
        Assembles the content of the tweet into a searchable data structure. All content in put into lowercase.

        :param tweet: The dictionary twitter object.
        :type tweet: dict
        :rtype: dict
        """
        searchable = {
            "hashtag": set(),
            "keyword": u"",
            "username": set(),
            "userid": set()
        }

        # Placeholder defaults to minimize allocations
        _d = dict()
        _l = list()
        _s = ''

        if self.traptor_type == 'track':

            free_text = {tweet.get('text', _s),
                         tweet.get('extended_tweet', _d).get('full_text', _s),
                         tweet.get('quoted_status', _d).get('extended_tweet', _d).get('full_text', _s),
                         tweet.get('quoted_status', _d).get('text', _s),
                         tweet.get('retweeted_status', _d).get('extended_tweet', _d).get('full_text', _s),
                         tweet.get('retweeted_status', _d).get('text', _s),
                         tweet.get('user', _d).get('screen_name', _s)}

            self._get_url_fields(tweet.get('entities', _d).get('urls', _l), free_text)
            self._get_url_fields(tweet.get('extended_tweet', _d).get('entities', _d).get('urls', _l), free_text)
            self._get_url_fields(tweet.get('retweeted_status', _d).get('extended_tweet', _d).get('entities', _d).get('urls', _l), free_text)
            self._get_url_fields(tweet.get('retweeted_status', _d).get('entities', _d).get('urls', _l), free_text)
            self._get_url_fields(tweet.get('quoted_status', _d).get('extended_tweet', _d).get('entities', _d).get('urls', _l), free_text)
            self._get_url_fields(tweet.get('quoted_status', _d).get('entities', _d).get('urls', _l), free_text)
            self._get_url_fields(tweet.get('extended_tweet', _d).get('entities', _d).get('media', _l), free_text)
            self._get_url_fields(tweet.get('entities', _d).get('media', _l), free_text)
            self._get_url_fields(tweet.get('retweeted_status', _d).get('extended_tweet', _d).get('entities', _d).get('media', _l), free_text)
            self._get_url_fields(tweet.get('retweeted_status', _d).get('entities', _d).get('media', _l), free_text)
            self._get_url_fields(tweet.get('quoted_status', _d).get('extended_tweet', _d).get('entities', _d).get('media', _l), free_text)
            self._get_url_fields(tweet.get('quoted_status', _d).get('entities', _d).get('media', _l), free_text)

            if _s in free_text:
                free_text.remove(_s)
            searchable['keyword'] = u" ".join(free_text).lower()

            for hashtag in tweet.get('extended_tweet', _d).get('entities', _d).get('hashtags', _l):
                if 'text' in hashtag and hashtag['text'] is not None:
                    searchable['hashtag'].add(hashtag.get('text').lower())

            for hashtag in tweet.get('entities', _d).get('hashtags', _l):
                if 'text' in hashtag and hashtag['text'] is not None:
                    searchable['hashtag'].add(hashtag.get('text').lower())

            for hashtag in tweet.get('retweeted_status', _d).get('extended_tweet', _d).get('entities', _d).get('hashtags', _l):
                if 'text' in hashtag and hashtag['text'] is not None:
                    searchable['hashtag'].add(hashtag.get('text').lower())

            for hashtag in tweet.get('retweeted_status', _d).get('entities', _d).get('hashtags', _l):
                if 'text' in hashtag and hashtag['text'] is not None:
                    searchable['hashtag'].add(hashtag.get('text').lower())

            for hashtag in tweet.get('quoted_status', _d).get('extended_tweet', _d).get('entities', _d).get('hashtags', _l):
                if 'text' in hashtag and hashtag['text'] is not None:
                    searchable['hashtag'].add(hashtag.get('text').lower())

            for hashtag in tweet.get('quoted_status', _d).get('entities', _d).get('hashtags', _l):
                if 'text' in hashtag and hashtag['text'] is not None:
                    searchable['hashtag'].add(hashtag.get('text').lower())

            if _s in searchable['hashtag']:
                searchable['hashtag'].remove(_s)

        elif self.traptor_type == 'follow':

            searchable['userid'].add(tweet.get('user', _d).get('id_str', _s))
            searchable['userid'].add(tweet.get('retweeted_status', _d).get('user', _d).get('id_str', _s))
            searchable['userid'].add(tweet.get('quoted_status', _d).get('user', _d).get('id_str', _s))

            for user_mention in tweet.get('entities', _d).get('user_mentions', _l):
                if 'id_str' in user_mention and user_mention['id_str'] is not None:
                    searchable['userid'].add(user_mention.get('id_str'))

            for user_mention in tweet.get('extended_tweet', _d).get('entities', _d).get('user_mentions', _l):
                if 'id_str' in user_mention and user_mention['id_str'] is not None:
                    searchable['userid'].add(user_mention.get('id_str'))

            for user_mention in tweet.get('retweeted_status', _d).get('extended_tweet', _d).get('entities', _d).get('user_mentions', _l):
                if 'id_str' in user_mention and user_mention['id_str'] is not None:
                    searchable['userid'].add(user_mention.get('id_str'))

            for user_mention in tweet.get('retweeted_status', _d).get('entities', _d).get('user_mentions', _l):
                if 'id_str' in user_mention and user_mention['id_str'] is not None:
                    searchable['userid'].add(user_mention.get('id_str'))

            for user_mention in tweet.get('quoted_status', _d).get('extended_tweet', _d).get('entities', _d).get('user_mentions', _l):
                if 'id_str' in user_mention and user_mention['id_str'] is not None:
                    searchable['userid'].add(user_mention.get('id_str'))

            for user_mention in tweet.get('quoted_status', _d).get('entities', _d).get('user_mentions', _l):
                if 'id_str' in user_mention and user_mention['id_str'] is not None:
                    searchable['userid'].add(user_mention.get('id_str'))

            if _s in searchable['userid']:
                searchable['userid'].remove(_s)

            searchable['username'].add(tweet.get('user', _d).get('screen_name', _s).lower())
            searchable['username'].add(tweet.get('retweeted_status', _d).get('user', _d).get('screen_name', _s).lower())
            searchable['username'].add(tweet.get('quoted_status', _d).get('user', _d).get('screen_name', _s).lower())

            for user_mention in tweet.get('entities', _d).get('user_mentions', _l):
                if 'screen_name' in user_mention and user_mention['screen_name'] is not None:
                    searchable['username'].add(user_mention.get('screen_name').lower())

            for user_mention in tweet.get('extended_tweet', _d).get('entities', _d).get('user_mentions', _l):
                if 'screen_name' in user_mention and user_mention['screen_name'] is not None:
                    searchable['username'].add(user_mention.get('screen_name').lower())

            for user_mention in tweet.get('retweeted_status', _d).get('extended_tweet', _d).get('entities', _d).get('user_mentions', _l):
                if 'screen_name' in user_mention and user_mention['screen_name'] is not None:
                    searchable['username'].add(user_mention.get('screen_name').lower())

            for user_mention in tweet.get('retweeted_status', _d).get('entities', _d).get('user_mentions', _l):
                if 'screen_name' in user_mention and user_mention['screen_name'] is not None:
                    searchable['username'].add(user_mention.get('screen_name').lower())

            for user_mention in tweet.get('quoted_status', _d).get('extended_tweet', _d).get('entities', _d).get('user_mentions', _l):
                if 'screen_name' in user_mention and user_mention['screen_name'] is not None:
                    searchable['username'].add(user_mention.get('screen_name').lower())

            for user_mention in tweet.get('quoted_status', _d).get('entities', _d).get('user_mentions', _l):
                if 'screen_name' in user_mention and user_mention['screen_name'] is not None:
                    searchable['username'].add(user_mention.get('screen_name').lower())

            if _s in searchable['username']:
                searchable['username'].remove(_s)

        return searchable

    def _find_rule_matches(self, tweet_dict):
        """
        Best effort search for rule matches for the tweet.

        :param tweet_dict: The dictionary twitter object.
        :type tweet_dict: dict
        :returns: a dict with the augmented data fields.
        :rtype: dict
        """
        new_dict = tweet_dict
        self.logger.debug('Finding tweet rule matches')

        collection_rules = list()
        tweet_dict['traptor']['collection_rules'] = collection_rules

        # If the Traptor is a geo traptor, return the one rule we've already set up
        if self.traptor_type == 'locations':
            rule = copy.deepcopy(self.locations_rule)
            collection_rules.append(rule)

        # Do track and follow Traptor rule tagging...
        elif self.traptor_type == 'track' or self.traptor_type == 'follow':
            try:
                content = self._build_match_content(tweet_dict)

                for rule in self.redis_rules:

                    rule_type = rule.get('orig_type')
                    rule_value = rule.get('value').lower()
                    value_terms = rule_value.split(" ")
                    matches = list()

                    for search_term in value_terms:
                        if rule_type in content:
                            matches.append(search_term in content[rule_type])
                        else:
                            pass

                    if len(matches) >=1 and all(matches):

                        match = copy.deepcopy(rule)

                        # These two lines kept for backwards compatibility
                        match['rule_tag'] = rule.get('tag')
                        match['rule_value'] = rule.get('value')

                        collection_rules.append(match)

                        # Log that a rule was matched
                        self.logger.debug('Rule matched', extra=logExtra({
                            'tweet id': tweet_dict.get('id_str'),
                            'rule_value': rule_value,
                            'rule_id': rule.get('rule_id')
                        }))

            except Exception as e:
                theLogMsg = "Caught exception while performing rule matching for " + self.traptor_type
                self.logger.error(theLogMsg, extra=logExtra(e))
                dd_monitoring.increment('traptor_error_occurred',
                                        tags=['error_type:rule_matching_failure'])

        else:
            self.logger.error("Ran into an unknown Traptor type...",
                              extra=logExtra({}))

        if len(collection_rules) == 0:
            new_dict['traptor']['rule_type'] = self.traptor_type
            new_dict['traptor']['id'] = int(self.traptor_id)
            new_dict['traptor']['rule_tag'] = 'Not Found'
            new_dict['traptor']['rule_value'] = 'Not Found'
            # Log that a rule was matched
            self.logger.warning("No rule matched for tweet", extra=logExtra({
                'tweet_id': tweet_dict['id_str']
            }))

        return new_dict

    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
        retry=retry_if_exception_type(redis.ConnectionError),
        after=log_retry_redis,
    )
    def _get_redis_rules(self):
        """ Yields a traptor rule from redis.  This function
            expects that the redis keys are set up like follows:

            traptor-<traptor_type>:<traptor_id>:<rule_id>

            For example,

            traptor-follow:0:34

            traptor-track:0:5

            traptor-locations:0:2

            For 'follow' twitter streaming, each traptor may only
            follow 5000 twitter ids, as per the Twitter API.

            For 'track' twitter stream, each traptor may only
            track 400 keywords, as per the Twitter API.

            For 'locations' twitter stream, each traptor may only
            track 25 bounding boxes, as per the Twitter API.

            :returns: Yields a traptor rule from redis.
        """
        # Set up API limitation checks
        if self.traptor_type == 'follow':
            rule_max = 5000
        elif self.traptor_type == 'track':
            rule_max = 400
        elif self.traptor_type == 'locations':
            rule_max = 1
        else:
            self.logger.error('Unsupported traptor_type', extra=logExtra({
                    'traptor_type': self.traptor_type
            }))
            dd_monitoring.increment('traptor_error_occurred',
                                    tags=['error_type:not_implemented_error'])
            raise NotImplementedError

        # for rule in xrange(rule_max):
        redis_key = 'traptor-{0}:{1}'.format(self.traptor_type,
                                             self.traptor_id)
        match = ':'.join([redis_key, '*'])
        try:
            self.logger.info("Getting rules from Redis", extra=logExtra())
            for idx, hashname in enumerate(self.redis_conn.scan_iter(match=match)):
                if idx < rule_max:
                    redis_rule = self.redis_conn.hgetall(hashname)
                    yield redis_rule
                    self.logger.debug('got from redis', extra=logExtra({
                            'index': idx,
                            'redis_rule': redis_rule
                    }))
        except Exception as e:
            theLogMsg = "Caught exception while getting rules from Redis"
            self.logger.critical(theLogMsg, extra=logExtra(e))
            dd_monitoring.increment('redis_error',
                                    tags=['error_type:connection_error'])

    @staticmethod
    def _tweet_time_to_iso(tweet_time):
        """
        Convert tweet created_at to ISO time format.

        :param tweet_time: created_at date of a tweet
        :return: A string of the ISO formatted time.
        """
        return parser.parse(tweet_time).isoformat()

    def _create_traptor_obj(self, tweet_dict):
        """
        Add the traptor dict and id to the tweet.

        :param tweet_dict: tweet in json format
        :return tweet_dict: with additional traptor fields
        """
        if 'traptor' not in tweet_dict:
            tweet_dict['traptor'] = {}
            tweet_dict['traptor']['id_str'] = int(self.traptor_id)

        return tweet_dict

    def _add_iso_created_at(self, tweet_dict):
        """
        Add the created_at_iso to the tweet.

        :param tweet_dict: tweet in json format
        :return tweet_dict: with created_at_iso field
        """
        if tweet_dict.get('created_at'):
            tweet_dict['traptor']['created_at_iso'] = self._tweet_time_to_iso(
                    tweet_dict['created_at']
            )

        return tweet_dict

    def _message_is_tweet(self, message):
        """
        Check if the message is a tweet.

        :param message: message to check
        :return: True if yes, False if no
        """
        if 'id_str' in message:
            return True
        else:
            return False

    def _message_is_limit_message(self, message):
        """
        Check if the message is a limit message.

        :param message: message to check
        :return: True if yes, False if no
        """
        if message.get('limit', None) is not None:
            return True
        else:
            return False

    def _enrich_tweet(self, tweet):
        """
        Enrich the tweet with additional fields, rule matching and stats collection.

        :param tweet: raw tweet info to enrich
        :return dict enriched_data: tweet dict with additional enrichments
        :return dict tweet: non-tweet message with no additional enrichments
        """
        enriched_data = dict()

        if self._message_is_limit_message(tweet):
            # Send DD the limit message value
            limit_count = tweet.get('limit').get(self.traptor_type, None)
            dd_monitoring.gauge('limit_message_count', limit_count, [])
            # Store the limit count in Redis
            self._increment_limit_message_counter(limit_count=limit_count)
            # Log limit message
            self.logger.info('limit_message_received', extra=logExtra({'limit_count': limit_count}))
        elif self._message_is_tweet(tweet):
            try:
                # Add the initial traptor fields
                tweet = self._create_traptor_obj(tweet)

                # Add the created_at_iso field
                tweet = self._add_iso_created_at(tweet)

                # Add the rule information
                enriched_data = self._find_rule_matches(tweet)

                # Update the matched rule stats
                if self.traptor_type != 'locations' \
                        and self.enable_stats_collection:
                    self._increment_rule_counter(enriched_data)
            except Exception as e:
                theLogMsg = "Failed to enrich tweet, skipping enhancement"
                self.logger.error(theLogMsg, extra=logExtra(e, {
                        "tweet": json.dumps(tweet)
                }))

                # an error occurred while processing the tweet. If some information was
                # set in the dictionary when calling _find_rule_matches, clear it out
                # because it is likely invalid...
                enriched_data = {}

        else:
            theLogMsg = "Twitter message is not a tweet"
            self.logger.info(theLogMsg, extra=logExtra({
                    'twitter_message': tweet
            }))

        dd_monitoring.increment('tweet_process_success')

        if enriched_data:
            return enriched_data
        else:
            return tweet

    def _listenToRedisForRestartFlag(self):
        """
        Listen to the Redis PubSub channel and set the restart flag for
        this Traptor if the restart message is found.
        """
        while not self.exit:
            pubsub = None
            try:
                theLogMsg = "Subscribing to Traptor notification PubSub"
                self.logger.info(theLogMsg, extra=logExtra({
                        'restart_flag': str(self._getRestartSearchFlag)
                }))

                pubsub = self.pubsub_conn.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.traptor_notify_channel)

                while not self.exit:
                    msg = pubsub.get_message(timeout=1)
                    if msg is not None:
                        data = str(msg['data'])
                        t = data.split(':')
                        self.logger.debug('PubSub', extra=logExtra(t))

                        if t[0] == self.traptor_type and t[1] == str(self.traptor_id):
                            # Restart flag found for our specific instance
                            self._setRestartSearchFlag(True)
                            self.logger.info('restart_message_received', extra=logExtra())
            except SystemExit:
                raise
            except Exception as e:
                self.logger.error("Exception caught in redis pub/sub listener, restarting listener", extra=logExtra(e))
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except SystemExit:
                        raise
                    except Exception as e:
                        self.logger.error("Exception caught closing redis pub/sub listener", extra=logExtra(e))

        self.logger.info("The redis pub/sub listener is exiting.", extra=logExtra())


    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
        retry=retry_if_exception_type(redis.ConnectionError),
        after=log_retry_redis,
    )
    def _add_heartbeat_message_to_redis(self, hb_interval):
        """Add a heartbeat message to Redis."""
        now = datetime.now().strftime("%Y%M%d%H%M%S")
        key_to_add = "{}:{}:{}".format(self.traptor_type,
                                       self.traptor_id,
                                       'heartbeat')
        message = now
        if self.heartbeat_conn.setex(key_to_add, int(hb_interval*1.5), message):
            theLogMsg = 'heartbeat_message_sent_success'
            self.logger.info(theLogMsg, extra=logExtra())

    def _send_heartbeat_message(self):
        """Add an expiring key to Redis as a heartbeat on a timed basis."""
        self.logger.info("Starting the heartbeat", extra=logExtra({
                'hb_interval': self._hb_interval()
        }))

        # while Traptor is running, add a heartbeat message every X seconds, min 5.
        while not self.exit:
            if self.accepting_assignments:
                try:
                    self._add_heartbeat_message_to_redis(self._hb_interval())
                except Exception as e:
                    theLogMsg = "Caught exception while adding the heartbeat message to Redis"
                    self.logger.error(theLogMsg, extra=logExtra(e))
            else:
                self.logger.info("Withholding heartbeat, not accepting rule assignments", extra=logExtra({}))

            self.exit_event.wait(self._hb_interval())

        self.logger.info("The heartbeat loop is exiting.", extra=logExtra())

    @retry(
        wait=wait_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
        retry=retry_if_exception_type(KafkaUnavailableError),
        after=log_retry_kafka,
    )
    def _send_enriched_data_to_kafka(self, tweet, enriched_data):
        """"
        Send the enriched data to Kafka

        :param tweet: the original tweet
        :param enriched_data: the enriched data to send
        """
        theLogMsg = "Attempting to send tweet to kafka"
        self.logger.info(theLogMsg, extra=logExtra({
            'tweet_id': tweet.get('id_str', None)
        }))

        try:
            future = self.kafka_conn.send(self.kafka_topic, enriched_data)
            future.add_callback(self.kafka_success_callback, tweet)
            future.add_errback(self.kafka_failure_callback)
        except Exception as e:
            self.logger.error("Kafka failed", extra=logExtra(e))

    def _filter_maintenance(self, t_now=time.time(), expiration_age_sec=60.0):
        """
        This examines each rule value we are tracking rates on for filtering and truncates
        any data that it outside the active window for evaluation. If all rate data is outside
        the window, then it means we haven't recently received traffic for that rule value and
        we can stop tracking it.
        :param t_now: The time at which to base maintenance from
        :type t_now: float
        :param expiration_age_sec: The length of time data is valid
        :type expiration_age_sec: float
        """
        expiration_time = t_now - expiration_age_sec
        keys = list(self.twitter_rate.keys())

        for key in keys:
            value = self.twitter_rate[key]

            # If the most recent value is too old, stop tracking the value
            if (value and value[-1] <= expiration_time) or not value:
                if key in self.kafka_rate:
                    del self.kafka_rate[key]

                if key in self.rate_limiter:
                    del self.rate_limiter[key]

                if key in self.twitter_rate:
                    del self.twitter_rate[key]
            else:
                # Drop old entries to stay within the expiration_age_sec
                while value and value[0] <= expiration_time:
                    value.popleft()

        for key, value in self.kafka_rate.items():
            while value and value[0] <= expiration_time:
                value.popleft()

    def _compute_rates(self, data, t_now, evaluation_window_sec):
        """

        :param data:
        :type data: dict
        :param t_now:
        :type t_now: float
        :param evaluation_window_sec:
        :type evaluation_window_sec: float
        :return:
        """
        rates = dict()

        if evaluation_window_sec <= 0.0:
            return rates

        t_start = t_now - evaluation_window_sec

        for key, value in data.items():

            current_data = deque(value)

            while current_data and current_data[0] < t_start:
                current_data.popleft()

            count = len(current_data)
            average_tps = float(count) / evaluation_window_sec

            max_tps = average_tps
            min_tps = average_tps

            if count > 0:
                second_buckets = dict()

                for i in range(int(evaluation_window_sec)):
                    second_buckets[i] = 0

                for timestamp in current_data:
                    second = int(timestamp - current_data[0])
                    if second not in second_buckets:
                        second_buckets[second] = 0
                    second_buckets[second] += 1

                for second, occurances in second_buckets.items():
                    max_tps = max(max_tps, float(occurances))
                    min_tps = min(min_tps, float(occurances))

            rates[key] = {
                'count': count,
                'max_tps': max_tps,
                'average_tps': average_tps,
                'min_tps': min_tps
            }

        return rates

    def _log_rates(self, t_now, evaluation_window_sec):
        """
        This computes the rate of traffic per rule value and logs the metrics.
        :param t_now: The time at which to base calculations from
        :type t_now: float
        :param evaluation_window_sec: The length of time data is evaluated
        :type evaluation_window_sec: float
        """

        for key, value in self._compute_rates(self.twitter_rate, t_now, evaluation_window_sec).items():

            self.logger.info("Twitter Rate", extra=logExtra(dict({
                'rule_value': key
            }, **value)))

        for key, value in self._compute_rates(self.kafka_rate, t_now, evaluation_window_sec).items():

            self.logger.info("Kafka Rate", extra=logExtra(dict({
                'rule_value': key
            }, **value)))

    def _is_filtered(self, enriched_data):
        """
        Tracks the volume of tweets per rule value received from twitter and applies a token bucket
        rate limiter to determine if we should send the tweet to kafka.

        :param enriched_data: The tweet with rule matches
        :return:
        """

        # set of keys -> rule_values
        rule_values = set()

        if 'traptor' in enriched_data and 'collection_rules' in enriched_data['traptor']:
            for rule in enriched_data['traptor']['collection_rules']:
                if 'value' in rule:
                    rule_values.add(RuleSet.get_normalized_value(rule))

        filtered = list()
        t_now = time.time()

        for key in rule_values:

            if self.rate_limiting_enabled and key not in self.rate_limiter:
                # Initialize a limiter for the untracked rule value
                storage = token_bucket.MemoryStorage()
                limiter = token_bucket.Limiter(self.rate_limiting_rate_sec, self.rate_limiting_capacity, storage)
                self.rate_limiter[key] = limiter

            if key not in self.twitter_rate:
                self.twitter_rate[key] = deque()

            self.twitter_rate[key].append(t_now)

            # Do we have enough token bucket credits (under the limit) to send the tweet?
            if not self.rate_limiting_enabled or self.rate_limiter[key].consume(key):

                if key not in self.kafka_rate:
                    self.kafka_rate[key] = deque()

                self.kafka_rate[key].append(t_now)
                filtered.append(False)
            else:
                filtered.append(True)

        # Ensure we don't filter tweets without any rules
        return len(filtered) != 0 and all(filtered)

    def _main_loop(self):
        """
        Main loop for iterating through the twitter data.

        This method iterates through the birdy stream, does any
        pre-processing, and adds enrichments to the data.  If kafka is
        enabled it will write to the kafka topic defined when instantiating
        the Traptor class.
        """
        self.logger.info("Starting tweet processing")
        # Iterate through the twitter results
        for item in self.birdy_stream.stream():
            if item:
                try:
                    tweet = json.loads(item)
                except Exception as e:
                    theLogMsg = "Caught exception while json loading the Twitter message"
                    self.logger.error(theLogMsg, extra=logExtra(e))
                    dd_monitoring.increment('traptor_error_occurred',
                                            tags=['error_type:json_loads_error'])
                else:
                    theLogMsg = "Enriching Tweet"
                    self.logger.debug(theLogMsg, extra=logExtra({
                        'tweet_id': tweet.get('id_str', None)
                    }))
                    enriched_data = self._enrich_tweet(tweet)

                    if not self._is_filtered(enriched_data):
                        # #4204 - since 1.4.13
                        theLogMsg = settings.DWC_SEND_TO_KAFKA_ENRICHED
                        self.logger.info(theLogMsg, extra=logExtra())
                        if self.kafka_enabled:
                            try:
                                self._send_enriched_data_to_kafka(tweet, enriched_data)
                            except Exception as e:
                                theLogMsg = settings.DWC_ERROR_SEND_TO_KAFKA
                                self.logger.error(theLogMsg, extra=logExtra(e))
                                dd_monitoring.increment('tweet_to_kafka_failure',
                                                        tags=['error_type:kafka'])
                        else:
                            self.logger.debug(json.dumps(enriched_data, indent=2))
                    else:
                        self.logger.debug("Tweet Rate Filtered", extra=logExtra({
                            'value_str': json.dumps(enriched_data, indent=2)
                        }))

            else:
                self.logger.info("Stream keep-alive received", extra=logExtra())

            t_now = time.time()

            if t_now > self._last_filter_maintenance + self.rate_limiting_reporting_interval_sec:
                self._log_rates(t_now, min(t_now - self._last_filter_maintenance, 2 * self.rate_limiting_reporting_interval_sec))
                self._filter_maintenance(t_now, self.rate_limiting_reporting_interval_sec)
                self._last_filter_maintenance = t_now

            if self.exit:
                break

            # Stop processing if we were told to restart
            if self._getRestartSearchFlag():
                self.logger.info("Restart flag is true; restarting myself", extra=logExtra())
                break

        self.logger.info("Stream iterator has exited.", extra=logExtra())

    def _wait_for_rules(self):
        """Wait for the Redis rules to appear"""
        # Get the list of rules from Redis
        self.redis_rules = [rule for rule in self._get_redis_rules()]

        # If there are no rules assigned to this Traptor, simma down and wait a minute
        while len(self.redis_rules) == 0 and not self.exit:
            self.logger.info('Waiting for rules', extra=logExtra({
                'sleep_seconds': self.rule_check_interval
            }))
            self.rule_wait_event.wait(self.rule_check_interval)
            self.rule_wait_event.clear()

            if self.exit:
                return

            self.redis_rules = [rule for rule in self._get_redis_rules()]

        # We got rules, tell my supervisors about them
        self.logger.info(settings.DWG_RULE_COUNT['key'], extra=logExtra({
                settings.DWG_RULE_COUNT['value']: len(self.redis_rules)
        }))

    def _handle_auth_error(self, error):

        # Until we have an automated way to get new creds, we will enter a state
        # where we regularly report that our creds are bad, terminate our
        # heartbeat so other working Traptors can service our rules, while we do nothing.
        self.accepting_assignments = False

        while not self.exit:
            self.logger.critical("API credentials are invalid", extra=logExtra(error))
            self.exit_event.wait(60)

        self.logger.info("The auth error blocking loop has exited", extra=logExtra())
        # TODO Use Keydat to acquire new API keys

    def run(self, args=None, aLogger=None):
        """
        Run method for running a traptor instance.
        It sets up the logging, connections, grabs the rules from redis,
        and starts writing data to kafka if enabled.

        :param args: CLI arguements, if any.
        """
        # Setup connections and logging
        self._setup(args, aLogger)

        # Create the thread for the pubsub restart check
        ps_check = threading.Thread(group=None,
                                    target=self._listenToRedisForRestartFlag
                                    )
        ps_check.setDaemon(True)
        ps_check.start()

        # Create the thread for the heartbeat message
        heartbeat = threading.Thread(group=None,
                                     target=self._send_heartbeat_message
                                     )
        heartbeat.setDaemon(True)
        heartbeat.start()

        self.logger.debug("Heartbeat started. Now to check for the rules")

        try:
            while not self.exit:
                self._delete_rule_counters()
                self._wait_for_rules()

                if self.exit:
                    break

                # Concatenate all of the rule['value'] fields
                self.twitter_rules = self._make_twitter_rules(self.redis_rules)

                if len(self.twitter_rules) == 0:
                    self.logger.warn('No valid Redis rules assigned', extra=logExtra({
                        'sleep_seconds': self.rule_check_interval
                    }))
                    self.exit_event.wait(self.rule_check_interval)
                    continue

                self.logger.debug('Twitter rules', extra=logExtra({
                        'dbg-rules': self.twitter_rules.encode('utf-8')
                }))

                # Make the rule and limit message counters
                if self.traptor_type != 'locations':
                    if self.enable_stats_collection:
                        self._make_rule_counters()
                    self._make_limit_message_counter()

                if not self.test and not self.exit:
                    try:
                        self._create_birdy_stream()
                    except TwitterAuthError as e:
                        self._handle_auth_error(e)

                if self.traptor_type == 'locations':
                    self.locations_rule = self._get_locations_traptor_rule()

                # reset Restart Search flag back to False
                self._setRestartSearchFlag(False)
                try:
                    if self.exit:
                        break

                    # Start collecting data
                    self._main_loop()
                except ChunkedEncodingError as e:
                    # Discussion online suggests this happens when we fall behind Twitter
                    # https://github.com/ryanmcgrath/twython/issues/288#issuecomment-66360160
                    theLogMsg = "Ran into a ChunkedEncodingError while processing "\
                        "tweets. Restarting Traptor from top of main process loop"
                    self.logger.error(theLogMsg, extra=logExtra(e))
                except (ConnectionError, Timeout) as e:
                    self.logger.error("Connection to Twitter broken.",
                                      extra=logExtra(e))
                finally:
                    try:
                        if self.birdy_stream:
                            self.birdy_stream.close()
                    except Exception as e:
                        self.logger.error("Could not close the stream connection.",
                                          extra=logExtra(e))
        finally:
            self.logger.info("Traptor has exited it's main loop", extra=logExtra())

            # Ensure wee notify the other threads
            if not self.exit:
                self._exit()

            try:
                self.logger.info("Joining redis pub-sub thread", extra=logExtra())
                ps_check.join(5)
            except Exception as e:
                self.logger.error("Failed to join redis pub-sub thread", extra=logExtra(e))

            try:
                self.logger.info("Joining heartbeat thread", extra=logExtra())
                heartbeat.join(5)
            except Exception as e:
                self.logger.info("Failed to join heartbeat thread", extra=logExtra(e))

            if self.kafka_conn is not None:
                try:
                    self.logger.info("Closing Kafka", extra=logExtra())
                    self.kafka_conn.close(5)
                except Exception as e:
                    self.logger.info("Exception closing Kafka", extra=logExtra(e))

            self.logger.info("Shutdown complete", extra=logExtra())


def sendRuleToRedis(aRedisConn, aRule, aRuleIndex=sys.maxint):
    aRedisConn.hmset('traptor-{0}:{1}:{2}'.format(
            my_traptor_type, my_traptor_id, aRuleIndex), aRule
    )


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


def getLoggingLevel(aLogLevelArg):
    """
    Get the logging level that should be reported.

    :param str aLogLevelArg: the CLI param
    :return: str: Returns one of the logging levels supported.
    """
    theLogLevel = getAppParamStr('LOG_LEVEL', settings.LOG_LEVEL, aLogLevelArg)
    if theLogLevel and theLogLevel.upper() in \
            ('CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'):
        theLogLevel = theLogLevel.upper()
    else:
        theLogLevel = 'INFO'
    return theLogLevel


def createArgumentParser():
    """
    Create and return the parser used for defining and processing CLI arguments.

    :return: ArgumentParser: returns the parser object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--delay',
            action='store_true',  # which defaults to False.
            help='Inserts an artificial delay to wait 30 seconds before startup.'
    )
    parser.add_argument(
            '--test',
            action='store_true',  # which defaults to False.
            help='Skips connecting to Twitter.'
    )
    parser.add_argument(
            '--loglevel',
            help='CRITICAL | ERROR | WARNING | INFO | DEBUG (case insensitive).'
    )
    parser.add_argument(
            '--log_file',
            help='Specify the LOG_FILE name.'
    )
    parser.add_argument(
            '--redis_pubsub',
            help='Specify the Traptor Redis PubSub channel.'
    )
    parser.add_argument(
            '--type',
            help='Specify the Traptor Type: track | follow | locations'
    )
    parser.add_argument(
            '--id',
            help='Specify the Traptor ID (an integer)'
    )
    parser.add_argument(
            '--interval',
            help='Specify the number of seconds between rule checks.'
    )
    parser.add_argument(
            '--kafka_enabled',
            help='Specify true for output to kafka or false for stdout.'
    )
    parser.add_argument(
            '--stats',
            help='Specify true or false for stats collection.'
    )
    parser.add_argument(
            '--log_stdout',
            help='Specify true to force logs to stdout.'
    )
    parser.add_argument(
            '--rule',
            help='Specify a rule to act upon for testing.'
    )
    parser.add_argument(
            '--heartbeat',
            help='Specify the number of seconds between heartbeat notifications.'
    )
    return parser


def main():
    """ Command line interface to run a traptor instance. """
    args = createArgumentParser().parse_args()

    global my_component
    whatisrunning = get_main()
    whatisextchar = whatisrunning.rfind('.')
    if whatisextchar > -1:
        whatisrunning = whatisrunning[:whatisextchar]
    my_component = whatisrunning
    global my_traptor_type
    my_traptor_type = getAppParamStr('TRAPTOR_TYPE', 'track', args.type)
    global my_traptor_id
    my_traptor_id = int(getAppParamStr('TRAPTOR_ID', '0', args.id))

    # Redis connections
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_db = int(os.getenv('REDIS_DB', 5))

    redis_conn = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db, decode_responses=True
    )

    # Redis pubsub connection
    pubsub_conn = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db
    )

    # Redis heartbeat connection
    heartbeat_conn = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db
    )

    # Twitter api keys
    if os.getenv('CONSUMER_KEY', '').startswith('ADD_'):
        api_keys = settings.APIKEYS
    else:
        api_keys = {
            'CONSUMER_KEY': os.getenv('CONSUMER_KEY'),
            'CONSUMER_SECRET': os.getenv('CONSUMER_SECRET'),
            'ACCESS_TOKEN': os.getenv('ACCESS_TOKEN'),
            'ACCESS_TOKEN_SECRET': os.getenv('ACCESS_TOKEN_SECRET')
        }
    if not api_keys or not api_keys['CONSUMER_KEY']:
        raise SystemExit('No API keys found')

    # Create the traptor instance
    traptor_instance = Traptor(
            redis_conn=redis_conn,
            pubsub_conn=pubsub_conn,
            heartbeat_conn=heartbeat_conn,
            traptor_notify_channel=getAppParamStr(
                'REDIS_PUBSUB_CHANNEL', 'traptor-notify', args.redis_pubsub
            ),
            rule_check_interval=int(getAppParamStr(
                    'RULE_CHECK_INTERVAL', '60', args.interval
            )),
            traptor_type=my_traptor_type,
            traptor_id=my_traptor_id,
            apikeys=api_keys,
            kafka_enabled=str2bool(getAppParamStr(
                    'KAFKA_ENABLED', 'true', args.kafka_enabled
            )),
            kafka_hosts=os.getenv('KAFKA_HOSTS', 'localhost:9092'),
            kafka_topic=os.getenv('KAFKA_TOPIC', 'traptor'),
            use_sentry=str2bool(os.getenv('USE_SENTRY', 'false')),
            sentry_url=os.getenv('SENTRY_URL', None),
            test=args.test,
            enable_stats_collection=str2bool(getAppParamStr(
                    'ENABLE_STATS_COLLECTION', 'true', args.stats
            )),
            heartbeat_interval=int(getAppParamStr(
                    'HEARTBEAT_INTERVAL', '0', args.heartbeat
            )),
            rate_limiting_enabled=str2bool(getAppParamStr(
                'RATE_LIMITING_ENABLED', settings.RATE_LIMITING_ENABLED
            )),
            rate_limiting_rate_sec=float(getAppParamStr(
                'RATE_LIMITING_RATE_SEC', settings.RATE_LIMITING_RATE_SEC
            )),
            rate_limiting_capacity=int(getAppParamStr(
                'RATE_LIMITING_CAPACITY', settings.RATE_LIMITING_CAPACITY
            )),
            rate_limiting_reporting_interval_sec=float(getAppParamStr(
                'RATE_LIMITING_REPORTING_INTERVAL_SEC', settings.RATE_LIMITING_REPORTING_INTERVAL_SEC
            )),
    )

    # Ensure we setup our CONSTS before we start actually doing things with threads
    dd_monitoring.DEFAULT_TAGS = [
            'traptor_type:{}'.format(traptor_instance.traptor_type),
            'traptor_id:{}'.format(traptor_instance.traptor_id),
            'traptor_version:{}'.format(version.__version__),
    ]
    global my_logger
    my_logger = LogFactory.get_instance(
            name=traptor_instance.name,
            json=str2bool(os.getenv('LOG_JSON', settings.LOG_JSON)),
            stdout=str2bool(getAppParamStr(
                    'LOG_STDOUT', settings.LOG_STDOUT, args.log_stdout
            )),
            level=getLoggingLevel(args.loglevel),
            dir=getAppParamStr('LOG_DIR', settings.LOG_DIR),
            file=getAppParamStr('LOG_FILE', settings.LOG_FILE, args.log_file)
    )

    if settings.DW_ENABLED:
        dw_config(settings.DW_CONFIG)
        my_logger.register_callback('>=INFO', dw_callback)

    # Wait until all the other containers are up and going...
    if args.delay:
        print('waiting 30 sec for other containers to get up and going...')
        time.sleep(30)

    # Run the traptor instance
    try:
        # Was a test rule passed in?
        if args.rule:
            sendRuleToRedis(redis_conn, {
                'rule_id': 'ARG-7777', 'tag': '7777-rule', 'value': args.rule
            })

        my_logger.info('Starting Traptor', extra=logExtra())
        my_logger.debug('Traptor', extra=logExtra(repr(traptor_instance)))
        traptor_instance.run(args, my_logger)
    except Exception as e:
        theLogMsg = 'Caught exception when running Traptor'
        my_logger.error(theLogMsg, extra=logExtra(e))
        dd_monitoring.increment('traptor_error_occurred',
                                tags=['error_type:traptor_start'])
        if str2bool(getAppParamStr('USE_SENTRY', 'false')):
            client = Client(getAppParamStr('SENTRY_URL'))
            client.captureException()
        raise e


if __name__ == '__main__':
    from raven import Client
    try:
        main()
    except KeyboardInterrupt:
        print("\n")
        sys.exit(0)

    # We should never leave main()
    sys.exit(1)
