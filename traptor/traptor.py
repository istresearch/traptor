#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import os
import time
import random
from datetime import datetime
# noinspection PyPackageRequirements
import dateutil.parser as parser
import traceback
import threading

import redis
import dd_monitoring
import six

# noinspection PyPackageRequirements
from kafka import KafkaProducer
# noinspection PyPackageRequirements
from kafka.common import KafkaUnavailableError

from birdy.twitter import TwitterApiError

from tenacity import retry, wait_exponential, stop_after_attempt, \
        retry_if_exception_type, wait_chain, wait_fixed

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
        self.use_sentry = use_sentry
        self.sentry_url = sentry_url
        self.test = test
        self.enable_stats_collection = enable_stats_collection
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
        self.logger.info('Setting up birdy connection')
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
                self.logger.critical(theLogMsg, extra=logExtra(e))
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

    @retry(
        wait=wait_chain(*[wait_fixed(3)] + [wait_fixed(7)] + [wait_fixed(9)]),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(TwitterApiError),
        reraise=True,
        after=log_retry_twitter,
    )
    def _create_twitter_follow_stream(self):
        """Create a Twitter follow stream."""
        self.logger.info('Creating birdy follow stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(
                follow=self.twitter_rules,
                stall_warnings='true'
        )

    @retry(
        wait=wait_chain(*[wait_fixed(3)] + [wait_fixed(7)] + [wait_fixed(9)]),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(TwitterApiError),
        reraise=True,
        after=log_retry_twitter,
    )
    def _create_twitter_track_stream(self):
        """Create a Twitter follow stream."""
        self.logger.info('Creating birdy track stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(
                track=self.twitter_rules,
                stall_warnings='true'
        )

    @retry(
        wait=wait_chain(*[wait_fixed(3)] + [wait_fixed(7)] + [wait_fixed(9)]),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(TwitterApiError),
        reraise=True,
        after=log_retry_twitter,
    )
    def _create_twitter_locations_stream(self):
        """Create a Twitter locations stream."""
        self.logger.info('Creating birdy locations stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(
                locations=self.twitter_rules,
                stall_warnings='true'
        )

    def _create_birdy_stream(self):
        """ Create a birdy twitter stream.
            If there is a TwitterApiError it will exit with status code 3.
            This was done to prevent services like supervisor from automatically
            restart the process causing the twitter API to get locked out.

            Creates ``self.birdy_stream``.
        """

        if self.traptor_type == 'follow':
            # Try to set up a twitter stream using twitter id list
            try:
                self._create_twitter_follow_stream()
            except Exception as e:
                theLogMsg = "Caught Twitter Api Error creating follow stream"
                self.logger.critical(theLogMsg, extra=logExtra(e, {
                    'retry': self._create_twitter_follow_stream.retry.statistics
                }))
                dd_monitoring.increment('twitter_error_occurred',
                                        tags=['error_type:twitter_api_error'])
                sys.exit(3)
        elif self.traptor_type == 'track':
            # Try to set up a twitter stream using twitter term list
            try:
                self._create_twitter_track_stream()
            except Exception as e:
                theLogMsg = "Caught Twitter Api Error creating track stream"
                self.logger.critical(theLogMsg, extra=logExtra(e))
                dd_monitoring.increment('twitter_error_occurred',
                                        tags=['error_type:twitter_api_error'])
                sys.exit(3)
        elif self.traptor_type == 'locations':
            # Try to set up a twitter stream using twitter term list
            try:
                self._create_twitter_locations_stream()
            except Exception as e:
                theLogMsg = "Caught Twitter Api Error creating locations stream"
                self.logger.critical(theLogMsg, extra=logExtra(e))
                dd_monitoring.increment('twitter_error_occurred',
                                        tags=['error_type:twitter_api_error'])
                sys.exit(3)
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
            sys.exit(3)

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
        rule_id = tweet.get('traptor', {}).get('rule_id', None)

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

    def _find_rule_matches(self, tweet_dict):
        """
        Find a rule match for the tweet.

        This code only expects there to be one match.  If there is more than
        one, it will use the last one it finds since the first match will be
        overwritten.

        :param dict tweet_dict: The dictionary twitter object.
        :returns: a ``dict`` with the augmented data fields.
        """
        new_dict = tweet_dict
        self.logger.debug('Finding tweet rule matches')

        # If the Traptor is a geo traptor, return the one rule we've already set up
        if self.traptor_type == 'locations':
            for key, value in self.locations_rule.iteritems():
                new_dict['traptor'][key] = value

        # Do track Traptor enrichments...
        elif self.traptor_type == 'track':

            """
            Here's how Twitter does it, and so shall we:

            The text of the Tweet and some entity fields are considered for matches.
            Specifically, the text attribute of the Tweet, expanded_url and display_url
            for links and media, text for hashtags, and screen_name for user mentions
            are checked for matches.
            """

            # Build up the query from our tweet fields
            query = ""

            # Tweet text
            query = query + tweet_dict['text'].encode("utf-8")

            # URLs and Media
            url_list = []
            if 'urls' in tweet_dict['entities']:
                for url in tweet_dict['entities']['urls']:
                    expanded_url = url.get('expanded_url', None)
                    display_url = url.get('display_url', None)

                    if expanded_url is not None:
                        url_list.append(expanded_url)
                    if display_url is not None:
                        url_list.append(display_url)

            if 'media' in tweet_dict['entities']:
                for item in tweet_dict['entities']['media']:
                    expanded_url = item.get('expanded_url', None)
                    display_url = item.get('display_url', None)

                    if expanded_url is not None:
                        url_list.append(expanded_url)
                    if display_url is not None:
                        url_list.append(display_url)

            # Hashtags
            if 'hashtags' in tweet_dict['entities']:
                for tag in tweet_dict['entities']['hashtags']:
                    query = query + " " + tag['text'].encode("utf-8")

            # Screen name
            if 'screen_name' in tweet_dict['user']:
                query = query + " " + tweet_dict['user']['screen_name'].encode('utf-8')

            # Retweeted parts
            if tweet_dict.get('retweeted_status', None) is not None:
                # Status
                query += " " + tweet_dict['retweeted_status']['text'].encode("utf-8")

                theFullText = tweet_dict['retweeted_status']\
                    .get('quoted_status', {})\
                    .get('extended_tweet', {})\
                    .get('full_text', None)
                if theFullText is not None:
                    query = " " + theFullText.encode("utf-8")

                # URLs and Media
                if 'urls' in tweet_dict['retweeted_status']['entities']:
                    for url in tweet_dict['retweeted_status']['entities']['urls']:
                        expanded_url = url.get('expanded_url', None)
                        display_url = url.get('display_url', None)

                        if expanded_url is not None:
                            url_list.append(expanded_url)
                        if display_url is not None:
                            url_list.append(display_url)

                if 'media' in tweet_dict['retweeted_status']['entities']:
                    for item in tweet_dict['retweeted_status']['entities']['media']:
                        expanded_url = item.get('expanded_url', None)
                        display_url = item.get('display_url', None)

                        if expanded_url is not None:
                            url_list.append(expanded_url)
                        if display_url is not None:
                            url_list.append(display_url)

                # Hashtags
                if 'hashtags' in tweet_dict['retweeted_status']['entities']:
                    for tag in tweet_dict['retweeted_status']['entities']['hashtags']:
                        query = query + " " + tag['text'].encode("utf-8")

                # Names
                if 'in_reply_to_screen_name' in tweet_dict['retweeted_status']:
                    in_reply_to_screen_name = tweet_dict.get('retweeted_status', {})\
                            .get('in_reply_to_screen_name', None)
                    if in_reply_to_screen_name is not None:
                        query += " " + tweet_dict['retweeted_status']['in_reply_to_screen_name'].encode('utf-8')

                if 'screen_name' in tweet_dict['retweeted_status']['user']:
                    screen_name = tweet_dict.get('retweeted_status', {}).get('user', {}).get('screen_name', None)
                    if screen_name is not None:
                        query += " " + tweet_dict['retweeted_status']['user']['screen_name'].encode('utf-8')

            # Quoted Status parts
            if tweet_dict.get('quoted_status', None) is not None:
                # Standard tweet
                if tweet_dict.get('quoted_status').get('text', None) is not None:
                    query = query + " " + tweet_dict['quoted_status']['text'].encode('utf-8')

                # Extended tweet
                if tweet_dict.get('quoted_status').get('extended_tweet', {}).get('full_text', None) is not None:
                    query = query + " " + tweet_dict['quoted_status']['extended_tweet']['full_text'].encode('utf-8')

            # De-dup urls and add to the giant query
            if len(url_list) > 0:
                url_list = set(url_list)
                for url in url_list:
                    query = query + " " + url.encode("utf-8")

            # Lowercase the entire thing
            query = query.lower()

            random.shuffle(self.redis_rules)

            try:
                # Shuffle the rules every once in a while
                for rule in self.redis_rules:
                    # Get the rule to search for and lowercase it
                    search_str = rule['value'].encode("utf-8").lower()

                    # Split the rule value and see if it's a multi-parter
                    part_finder = list()
                    search_str_multi = search_str.split(" ")

                    # If there is more than one part to the rule, check for each part in the query
                    if len(search_str_multi) > 1:
                        for part in search_str_multi:
                            if part in query:
                                part_finder.append(True)
                            else:
                                part_finder.append(False)

                    if len(search_str_multi) > 1 and all(part_finder):
                        # These two lines kept for backwards compatibility
                        new_dict['traptor']['rule_tag'] = rule['tag']
                        new_dict['traptor']['rule_value'] = rule['value'].encode("utf-8")

                        # Pass all key/value pairs from matched rule through to Traptor
                        for key, value in rule.iteritems():
                            new_dict['traptor'][key] = value.encode("utf-8")

                        # Log that a rule was matched
                        self.logger.debug('Rule matched', extra=logExtra({
                                'tweet id': tweet_dict['id_str']
                        }))

                    elif search_str in query:
                        # These two lines kept for backwards compatibility
                        new_dict['traptor']['rule_tag'] = rule['tag']
                        new_dict['traptor']['rule_value'] = rule['value'].encode("utf-8")

                        # Pass all key/value pairs from matched rule through to Traptor
                        for key, value in rule.iteritems():
                            new_dict['traptor'][key] = value.encode("utf-8")

                        # Log that a rule was matched
                        self.logger.debug('Rule matched', extra=logExtra({
                                'tweet id': tweet_dict['id_str']
                        }))
            except Exception as e:
                theLogMsg = "Caught exception while performing rule matching for track"
                self.logger.error(theLogMsg, extra=logExtra(e))
                dd_monitoring.increment('traptor_error_occurred',
                                        tags=['error_type:rule_matching_failure'])

        # If this is a follow Traptor, only check the user/id field of the tweet
        elif self.traptor_type == 'follow':
            """
            Here's how Twitter does it, and so shall we:

            Tweets created by the user.
            Tweets which are retweeted by the user.
            Replies to any Tweet created by the user.
            Retweets of any Tweet created by the user.
            Manual replies, created without pressing a reply button (e.g. “@twitterapi I agree”).
            """

            # Build up the query from our tweet fields
            query = ""

            # Tweets created by the user AND
            # Tweets which are retweeted by the user

            try:
                theLogMsg = 'tweet_dict for rule match'
                self.logger.debug(theLogMsg, extra=logExtra({
                        'tweet_dict': json.dumps(tweet_dict).encode("utf-8")
                }))
            except Exception as e:
                theLogMsg = "Unable to dump the tweet dict to json"
                self.logger.error(theLogMsg, extra=logExtra(e))
                dd_monitoring.increment('traptor_error_occurred',
                                        tags=['error_type:json_dumps'])

            # From this user
            query += str(tweet_dict['user']['id_str'])

            # Replies to any Tweet created by the user.
            if tweet_dict['in_reply_to_user_id'] is not None \
                    and tweet_dict['in_reply_to_user_id'] != '':
                query += str(tweet_dict['in_reply_to_user_id'])

            # User mentions
            if 'user_mentions' in tweet_dict['entities']:
                for tag in tweet_dict['entities']['user_mentions']:
                    id_str = tag.get('id_str')
                    if id_str:
                        query = query + " " + id_str.encode("utf-8")

            # Retweeted parts
            if tweet_dict.get('retweeted_status', None) is not None:
                if tweet_dict['retweeted_status'].get('user', {}).get('id_str', None) is not None:
                    query += str(tweet_dict['retweeted_status']['user']['id_str'])

            # Retweets of any Tweet created by the user; AND
            # Manual replies, created without pressing a reply button (e.g. “@twitterapi I agree”).
            query = query + tweet_dict['text'].encode("utf-8")

            # Lowercase the entire thing
            query = query.lower()

            random.shuffle(self.redis_rules)

            try:
                for rule in self.redis_rules:
                    # Get the rule to search for and lowercase it
                    search_str = str(rule['value']).encode("utf-8").lower()

                    self.logger.debug('rule matching', extra=logExtra({
                            'dbg-search': search_str,
                            'dbg-query': query
                    }))

                    if search_str in query:
                        # These two lines kept for backwards compatibility
                        new_dict['traptor']['rule_tag'] = rule['tag']
                        new_dict['traptor']['rule_value'] = rule['value'].encode("utf-8")

                        # Pass all key/value pairs from matched rule through to Traptor
                        for key, value in rule.iteritems():
                            new_dict['traptor'][key] = value.encode("utf-8")

                        # Log that a rule was matched
                        self.logger.debug('rule matched', extra=logExtra({
                                'tweet id': tweet_dict['id_str']
                        }))
            except Exception as e:
                theLogMsg = "Caught exception while performing rule matching for follow"
                self.logger.error(theLogMsg, extra=logExtra(e))
                dd_monitoring.increment('traptor_error_occurred',
                                        tags=['error_type:rule_matching_failure'])

        # unknown traptor type
        else:
            self.logger.warning("Ran into an unknown Traptor type...")

        if 'rule_tag' not in new_dict['traptor']:
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
        theLogMsg = "Subscribing to Traptor notification PubSub"
        self.logger.info(theLogMsg, extra=logExtra({
                'restart_flag': str(self._getRestartSearchFlag)
        }))

        p = self.pubsub_conn.pubsub(ignore_subscribe_messages=True)
        p.subscribe(self.traptor_notify_channel)
        # listen() is a generator that blocks until a message is available
        for msg in p.listen():
            if msg is not None:
                data = str(msg['data'])
                t = data.split(':')
                self.logger.debug('PubSub', extra=logExtra(t))
                if t[0] == self.traptor_type and t[1] == str(self.traptor_id):
                    # Restart flag found for our specific instance
                    self._setRestartSearchFlag(True)
                    self.logger.info('restart_message_received', extra=logExtra())

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
            # DEBUG: send Restart Flag to myself to test Restart Flag action (not thread-safe!)
            # self.heartbeat_conn.publish(self.traptor_notify_channel, self.traptor_type+':'+str(self.traptor_id))

    def _send_heartbeat_message(self):
        """Add an expiring key to Redis as a heartbeat on a timed basis."""
        self.logger.info("Starting the heartbeat", extra=logExtra({
                'hb_interval': self._hb_interval()
        }))

        # while Traptor is running, add a heartbeat message every X seconds, min 5.
        while True:
            try:
                self._add_heartbeat_message_to_redis(self._hb_interval())
            except Exception as e:
                theLogMsg = "Caught exception while adding the heartbeat message to Redis"
                self.logger.error(theLogMsg, extra=logExtra(e))
                raise e

            time.sleep(self._hb_interval())

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
        future = self.kafka_conn.send(self.kafka_topic, enriched_data)
        future.add_callback(self.kafka_success_callback, tweet)
        future.add_errback(self.kafka_failure_callback)

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
                    self.logger.info(theLogMsg, extra=logExtra({
                        'tweet_id': tweet.get('id_str', None)
                    }))
                    enriched_data = self._enrich_tweet(tweet)
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
                self.logger.info("Stream keep-alive received", extra=logExtra())

            # Stop processing if we were told to restart
            if self._getRestartSearchFlag():
                self.logger.info("Restart flag is true; restarting myself", extra=logExtra())
                break

        self.logger.info("Stream iterator has exited.", extra=logExtra())

    def _wait_for_rules(self):
        """Wait for the Redis rules to appear"""
        # Get the list of rules from Redis
        self.redis_rules = [rule for rule in self._get_redis_rules()]

        if len(self.redis_rules) == 0:
            self.logger.info('Waiting for rules', extra=logExtra())

        # If there are no rules assigned to this Traptor, simma down and wait a minute
        while len(self.redis_rules) == 0:
            self.logger.debug('No Redis rules assigned', extra=logExtra({
                    'sleep_seconds': self.rule_check_interval
            }))
            time.sleep(self.rule_check_interval)
            self.redis_rules = [rule for rule in self._get_redis_rules()]

        # We got rules, tell my supervisors about them
        self.logger.info(settings.DWG_RULE_COUNT['key'], extra=logExtra({
                settings.DWG_RULE_COUNT['value']: len(self.redis_rules)
        }))

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

        # Do all the things
        while True:
            self._delete_rule_counters()
            self._wait_for_rules()

            # Concatenate all of the rule['value'] fields
            self.twitter_rules = self._make_twitter_rules(self.redis_rules)

            if len(self.twitter_rules) == 0:
                self.logger.warn('No valid Redis rules assigned', extra=logExtra({
                    'sleep_seconds': self.rule_check_interval
                }))
                time.sleep(self.rule_check_interval)
                continue

            self.logger.debug('Twitter rules', extra=logExtra({
                    'dbg-rules': self.twitter_rules.encode('utf-8')
            }))

            # Make the rule and limit message counters
            if self.traptor_type != 'locations':
                if self.enable_stats_collection:
                    self._make_rule_counters()
                self._make_limit_message_counter()

            if not self.test:
                self._create_birdy_stream()

            if self.traptor_type == 'locations':
                self.locations_rule = self._get_locations_traptor_rule()

            # reset Restart Search flag back to False
            self._setRestartSearchFlag(False)
            try:
                # Start collecting data
                self._main_loop()
            except ChunkedEncodingError as e:
                theLogMsg = "Ran into a ChunkedEncodingError while processing "\
                    "tweets. Restarting Traptor from top of main process loop"
                self.logger.error(theLogMsg, extra=logExtra(e))
            except (ConnectionError, Timeout) as e:
                self.logger.error("Connection to Twitter broken.",
                                  extra=logExtra(e))
            finally:
                try:
                    self.birdy_stream.close()
                except Exception as e:
                    self.logger.error("Could not close the stream connection.",
                                      extra=logExtra(e))


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
            ))
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
    else:
        print('skipping artificial delay')

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
