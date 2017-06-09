#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import os
import time
import random
from datetime import datetime
import dateutil.parser as parser
import traceback

import redis
from kafka import KafkaProducer
from kafka.common import KafkaUnavailableError
from birdy.twitter import StreamClient, TwitterApiError
import dd_monitoring

import threading
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, wait_chain, wait_fixed

from dog_whistle import dw_config, dw_callback
from requests.exceptions import ChunkedEncodingError

from scutils.log_factory import LogFactory
from scutils.stats_collector import StatsCollector
from traptor_limit_counter import TraptorLimitCounter

import logging
import settings

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level='INFO', format=FORMAT)

# Override the default JSONobject
class MyBirdyClient(StreamClient):
    @staticmethod
    def get_json_object_hook(data):
        return data

class Traptor(object):

    def __init__(self,
                 redis_conn,
                 pubsub_conn,
                 heartbeat_conn,
                 traptor_notify_channel='traptor-notify',
                 rule_check_interval=60,
                 traptor_type='track',
                 traptor_id=0,
                 apikeys=None,
                 kafka_enabled='true',
                 kafka_hosts='localhost:9092',
                 kafka_topic='traptor',
                 use_sentry='false',
                 sentry_url=None,
                 test=False,
                 enable_stats_collection='true',
                 ):
        """
        Traptor base class.

        :param str redis_conn: redis connection to use.
        :param str pubsub_conn: redis pubsub connection to use
        :param str heartbeat_conn: redis connection to use for the heartbeat messages
        :param str traptor_notify_channel: name of the traptor PubSub channel to subscribe to
        :param str rule_check_interval: number of seconds between checks of Redis for rules assigned to this traptor
        :param str traptor_type: follow, track, or geo.
        :param int traptor_id: numerical ID of traptor instance.
        :param dict apikeys: dictionary of API keys for traptor instnace.  See
                             settings.py for details.
        :param bool kafka_enabled: write to kafka or just log to something else.
        :param str kafka_hosts: kafka hosts to connect to.
        :param str kafka_topic: name of the kafka topic to write to.
        :param str use_sentry: whether or not to use Sentry for error reporting
        :param str sentry_url: url for Sentry logging
        :param bool test: True for traptor test instance.
        :param str enable_stats_collection: Whether or not to allow redis stats collection
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

        self.kafka_success_callback = self._gen_kafka_success()
        self.kafka_failure_callback = self._gen_kafka_failure()

        self.rule_counters = dict()
        self.limit_counter = None

    def __repr__(self):
        return 'Traptor({}, {}, {}, {}, {}, {}, {}, {}, {}, {} ,{}, {}, {}, {})'.format(
            self.redis_conn,
            self.pubsub_conn,
            self.heartbeat_conn,
            self.traptor_notify_channel,
            self.rule_check_interval,
            self.traptor_type,
            self.traptor_id,
            self.apikeys,
            self.kafka_enabled,
            self.kafka_hosts,
            self.kafka_topic,
            self.use_sentry,
            self.sentry_url,
            self.test
        )

    def _setup_birdy(self):
        """ Set up a birdy twitter stream.
            If there is a TwitterApiError it will exit with status code 3.
            This was done to prevent services like supervisor from automatically
            restart the process causing the twitter API to get locked out.

            Creates ``self.birdy_conn``.
        """

        # Set up a birdy twitter streaming client
        self.logger.info('Setting up birdy connection')
        self.birdy_conn = MyBirdyClient(
                                        self.apikeys['CONSUMER_KEY'],
                                        self.apikeys['CONSUMER_SECRET'],
                                        self.apikeys['ACCESS_TOKEN'],
                                        self.apikeys['ACCESS_TOKEN_SECRET']
                                        )

    @retry(wait=wait_exponential(multiplier=1, max=10),
           stop=stop_after_attempt(3),
           retry=retry_if_exception_type(KafkaUnavailableError)
           )
    def _create_kafka_producer(self):
        """Create the Kafka producer"""
        self.kafka_conn = KafkaProducer(bootstrap_servers=self.kafka_hosts,
                                        value_serializer=lambda m: json.dumps(m),
                                        api_version=(0, 9),
                                        reconnect_backoff_ms=4000,
                                        retries=3,
                                        linger_ms=25,
                                        buffer_memory=4 * 1024 * 1024)

    def _setup_kafka(self):
        """ Set up a Kafka connection."""
        if self.kafka_enabled == 'true':
            self.logger.info('Setting up kafka connection')
            try:
                self._create_kafka_producer()
            except:
                self.logger.critical("Caught Kafka Unavailable Error", extra={
                    'error_type': 'KafkaUnavailableError',
                    'ex': traceback.format_exc()
                })
                dd_monitoring.increment('kafka_error',
                                        tags=['error_type:kafka_unavailable'])
                # sys.exit(3)
        else:
            self.logger.info('Skipping kafka connection setup')
            self.logger.debug('Kafka_enabled setting: {}'.format(self.kafka_enabled))
            self.kafka_conn = None

    def _gen_kafka_success(self):
        def kafka_success(tweet, response):
            self.logger.info("Tweet sent to kafka", extra={
                'tweet_id': tweet.get('id_str', None)
            })
            dd_monitoring.increment('tweet_to_kafka_success')
        return kafka_success

    def _gen_kafka_failure(self):
        def kafka_failure(e):
            self.logger.error("Caught Kafka exception when sending a tweet to Kafka", extra={
                'error_type': 'KafkaError',
                'ex': traceback.format_exc()
            })
            dd_monitoring.increment('tweet_to_kafka_failure',
                                    tags=['error_type:kafka'])
        return kafka_failure

    def _setup(self):
        """
        Set up Traptor.

        Load everything up. Note that any arg here will override both
        default and custom settings.
        """

        traptor_name = 'traptor-{}-{}'.format(os.getenv('TRAPTOR_TYPE', 'track'),
                                              os.getenv('TRAPTOR_ID', 0))

        # Set up logging
        self.logger = LogFactory.get_instance(json=os.getenv('LOG_JSON', settings.LOG_JSON) == 'True',
                    name=os.getenv('LOG_NAME', settings.LOG_NAME),
                    stdout=os.getenv('LOG_STDOUT', settings.LOG_STDOUT) == 'True',
                    level=os.getenv('LOG_LEVEL', settings.LOG_LEVEL),
                    dir=os.getenv('LOG_DIR', settings.LOG_DIR),
                    file=os.getenv('LOG_FILE', settings.LOG_FILE))

        if settings.DW_ENABLED:
            dw_config(settings.DW_CONFIG)
            self.logger.register_callback('>=INFO', dw_callback)

        # Set the restart_flag to False
        self.restart_flag = False

        # Set up required connections
        self._setup_kafka()
        self._setup_birdy()

        # Create the locations_rule dict if this is a locations traptor
        self.locations_rule = {}

    @retry(wait=wait_chain(*[wait_fixed(3)] + [wait_fixed(7)] + [wait_fixed(9)]),
           stop=stop_after_attempt(3),
           retry=retry_if_exception_type(TwitterApiError)
           )
    def _create_twitter_follow_stream(self):
        """Create a Twitter follow stream."""
        self.logger.info('Creating birdy follow stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(follow=self.twitter_rules,
                                                                        stall_warnings='true')

    @retry(wait=wait_chain(*[wait_fixed(3)] + [wait_fixed(7)] + [wait_fixed(9)]),
           stop=stop_after_attempt(3),
           retry=retry_if_exception_type(TwitterApiError)
           )
    def _create_twitter_track_stream(self):
        """Create a Twitter follow stream."""
        self.logger.info('Creating birdy track stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(track=self.twitter_rules,
                                                                        stall_warnings='true')

    @retry(wait=wait_chain(*[wait_fixed(3)] + [wait_fixed(7)] + [wait_fixed(9)]),
           stop=stop_after_attempt(3),
           retry=retry_if_exception_type(TwitterApiError)
           )
    def _create_twitter_locations_stream(self):
        """Create a Twitter locations stream."""
        self.logger.info('Creating birdy locations stream')
        self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(locations=self.twitter_rules,
                                                                        stall_warnings='true')

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
            except TwitterApiError as e:
                self.logger.critical("Caught Twitter Api Error creating follow stream", extra = {
                    'error_type': 'TwitterAPIError',
                    'ex': traceback.format_exc()
                })
                dd_monitoring.increment('twitter_error_occurred',
                                        tags=['error_type:twitter_api_error'])
                sys.exit(3)
        elif self.traptor_type == 'track':
            # Try to set up a twitter stream using twitter term list
            try:
                self._create_twitter_track_stream()
            except TwitterApiError as e:
                self.logger.critical("Caught Twitter Api Error", extra={
                    'error_type': 'TwitterAPIError',
                    'ex': traceback.format_exc()
                })
                dd_monitoring.increment('twitter_error_occurred',
                                        tags=['error_type:twitter_api_error'])
                sys.exit(3)
        elif self.traptor_type == 'locations':
            # Try to set up a twitter stream using twitter term list
            try:
                self._create_twitter_locations_stream()
            except TwitterApiError as e:
                self.logger.critical("Caught Twitter Api Error", extra={
                    'error_type': 'TwitterAPIError',
                    'ex': traceback.format_exc()
                })
                dd_monitoring.increment('twitter_error_occurred',
                                        tags=['error_type:twitter_api_error'])
                sys.exit(3)
        else:
            self.logger.critical('Caught error creating birdy stream for Traptor type that does not exist', extra ={
                'error_type': 'NotImplementedError',
                'ex': traceback.format_exc()
            })
            dd_monitoring.increment('traptor_error_occurred',
                                    tags=['error_type:not_implemented_error'])
            sys.exit(3)

    def _make_twitter_rules(self, rules):
        """ Convert the rules from redis into a format compatible with the
            Twitter API.

            :param list rules: The rules are expected to be a list of
                                dictionaries that comes from redis.
            :returns: A ``str`` of twitter rules that can be loaded into the
                      a birdy twitter stream.
        """
        rules_str = ','.join([rule['value'] for rule in rules])
        self.logger.debug('Twitter rules string: {}'.format(rules_str.encode('utf-8')))
        return rules_str

    def _create_rule_counter(self, rule_id):
        """
        Create a rule counter

        :param rule_id: id of the rule to create a counter for
        :return: stats_collector: StatsCollector rolling time window
        """
        collection_window = int(os.getenv('STATS_COLLECTION_WINDOW', 900))
        stats_key = 'stats:{}:{}:{}'.format(self.traptor_type, self.traptor_id, rule_id)
        stats_collector = StatsCollector.get_rolling_time_window(redis_conn=self.redis_conn,
                                                                 key=stats_key,
                                                                 window=collection_window)

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

    @retry(wait=wait_exponential(multiplier=1, max=10),
           stop=stop_after_attempt(3),
           reraise=True,
           retry=retry_if_exception_type(redis.ConnectionError)
           )
    def _increment_rule_counter(self, tweet):
        """
        Increment a rule counter.

        :param rule_value: the value of the rule to increment the counter for
        """
        rule_id = tweet.get('traptor', {}).get('rule_id', None)

        # If the counter doesn't yet exist, create it
        if self.rule_counters.get(rule_id, None) is None:
            self.rule_counters[rule_id] = self._create_rule_counter(rule_id=rule_id)

        # If a rule value exists, increment the counter
        try:
            if rule_id is not None and self.rule_counters[rule_id] is not None:
                self.rule_counters[rule_id].increment()
        except:
            self.logger.error("Caught exception while incrementing a rule counter", extra={
                'error_type': 'RedisConnectionError',
                'ex': traceback.format_exc()
            })
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
                except:
                    self.logger.error("Caught exception while deactivating a rule counter", extra={
                        'error_type': 'ConnectionError',
                        'ex': traceback.format_exc()
                    })
                    dd_monitoring.increment('redis_error',
                                            tags=['error_type:connection_error'])
            for counter in self.rule_counters:
                try:
                    self.rule_counters[counter].stop()
                    self.rule_counters[counter].delete_key()
                except:
                    self.logger.error("Caught exception while stopping and deleting a rule counter", extra={
                        'error_type': 'RedisConnectionError',
                        'ex': traceback.format_exc()
                    })
                    dd_monitoring.increment('redis_error',
                                            tags=['error_type:connection_error'])
            self.logger.info("Rule counters deleted successfully")

    def _make_limit_message_counter(self):
        """
        Make a limit message counter to track the values of incoming limit messages.
        """
        limit_counter_key = "limit:{}:{}".format(self.traptor_type, self.traptor_id)
        collection_window = int(os.getenv('LIMIT_COUNT_COLLECTION_WINDOW', 900))

        self.limit_counter = TraptorLimitCounter(key=limit_counter_key, window=collection_window)
        self.limit_counter.setup(redis_conn=self.redis_conn)

    @retry(wait=wait_exponential(multiplier=1, max=10),
           stop=stop_after_attempt(3),
           reraise=True,
           retry=retry_if_exception_type(redis.ConnectionError)
           )
    def _increment_limit_message_counter(self, limit_count):
        """
        Increment the limit message counter

        :param limit_count: the integer value from the limit message
        """
        try:
            if self.limit_counter is not None:
                self.limit_counter.increment(limit_count=limit_count)
        except:
            self.logger.error("Caught exception while incrementing a limit counter", extra={
                'error_type': 'RedisConnectionError',
                'ex': traceback.format_exc()
            })
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

        This code only expects there to be one match.  If there is more than one, it will use the last one
        it finds since the first match will be overwritten.

        :param dict tweet_dict: The dictionary twitter object.
        :returns: a ``dict`` with the augmented data fields.
        """

        # If the traptor is any other type, keep it going
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
                query = query + " " + tweet_dict['retweeted_status']['text'].encode("utf-8")

                if tweet_dict['retweeted_status'].get('quoted_status', {}).get('extended_tweet', {}).get('full_text', None) is not None:
                    query = query + " " + tweet_dict['retweeted_status']['quoted_status']['extended_tweet']['full_text'].encode("utf-8")

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
                    in_reply_to_screen_name = tweet_dict.get('retweeted_status', {}).get('in_reply_to_screen_name', None)
                    if in_reply_to_screen_name is not None:
                        query = query + " " + tweet_dict['retweeted_status']['in_reply_to_screen_name'].encode('utf-8')

                if 'screen_name' in tweet_dict['retweeted_status']['user']:
                    screen_name = tweet_dict.get('retweeted_status', {}).get('user', {}).get('screen_name', None)
                    if screen_name is not None:
                        query = query + " " + tweet_dict['retweeted_status']['user']['screen_name'].encode('utf-8')

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
                        self.logger.debug("Rule matched for tweet id: {}".format(tweet_dict['id_str']))

                    elif search_str in query:
                        # These two lines kept for backwards compatibility
                        new_dict['traptor']['rule_tag'] = rule['tag']
                        new_dict['traptor']['rule_value'] = rule['value'].encode("utf-8")

                        # Pass all key/value pairs from matched rule through to Traptor
                        for key, value in rule.iteritems():
                            new_dict['traptor'][key] = value.encode("utf-8")

                        # Log that a rule was matched
                        self.logger.debug("Rule matched for tweet id: {}".format(tweet_dict['id_str']))
            except:
                self.logger.error("Caught exception while performing rule matching for track", extra={
                    'ex': traceback.format_exc()
                })
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
                self.logger.debug('tweet_dict for rule match',
                                  extra={'tweet_dict': json.dumps(tweet_dict).encode("utf-8")})
            except:
                self.logger.error("Unable to dump the tweet dict to json", extra={
                    'ex': traceback.format_exc()
                })
                dd_monitoring.increment('traptor_error_occurred',
                                        tags=['error_type:json_dumps'])

            # From this user
            query = query + str(tweet_dict['user']['id_str'])

            # Replies to any Tweet created by the user.
            if tweet_dict['in_reply_to_user_id'] is not None and tweet_dict['in_reply_to_user_id'] != '':
                query = query + str(tweet_dict['in_reply_to_user_id'])

            # User mentions
            if 'user_mentions' in tweet_dict['entities']:
                for tag in tweet_dict['entities']['user_mentions']:
                    id_str = tag.get('id_str')
                    if id_str:
                        query = query + " " + id_str.encode("utf-8")


            # Retweeted parts
            if tweet_dict.get('retweeted_status', None) is not None:
                if tweet_dict['retweeted_status'].get('user', {}).get('id_str', None) is not None:
                    query = query + str(tweet_dict['retweeted_status']['user']['id_str'])

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

                    self.logger.debug("Search string used for the rule match: {}".format(search_str))
                    self.logger.debug("Query for the rule match: {}".format(query))

                    if search_str in query:
                        # These two lines kept for backwards compatibility
                        new_dict['traptor']['rule_tag'] = rule['tag']
                        new_dict['traptor']['rule_value'] = rule['value'].encode("utf-8")

                        # Pass all key/value pairs from matched rule through to Traptor
                        for key, value in rule.iteritems():
                            new_dict['traptor'][key] = value.encode("utf-8")

                        # Log that a rule was matched
                        self.logger.debug("Rule matched for tweet id: {}".format(tweet_dict['id_str']))
            except:
                self.logger.error("Caught exception while performing rule matching for follow", extra={
                    'ex': traceback.format_exc()
                })
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
            self.logger.warning("No rule matched for tweet", extra={
                'tweet_id': tweet_dict['id_str']
            })

        return new_dict

    @retry(wait=wait_exponential(multiplier=1, max=10),
           stop=stop_after_attempt(3),
           reraise=True,
           retry=retry_if_exception_type(redis.ConnectionError)
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
            self.logger.error('Unsupported traptor_type', extra={'traptor_type': self.traptor_type})
            dd_monitoring.increment('traptor_error_occurred',
                                    tags=['error_type:not_implemented_error'])
            raise(NotImplementedError)

        # for rule in xrange(rule_max):
        redis_key = 'traptor-{0}:{1}'.format(self.traptor_type,
                                             self.traptor_id)
        match = ':'.join([redis_key, '*'])
        try:
            self.logger.info("Getting rules from Redis")
            for idx, hashname in enumerate(self.redis_conn.scan_iter(match=match)):
                if idx < rule_max:
                    redis_rule = self.redis_conn.hgetall(hashname)
                    yield redis_rule
                    self.logger.debug('Index: {0}, Redis_rule: {1}'.format(
                                      idx, redis_rule))
        except:
            self.logger.critical("Caught exception while connecting to Redis", extra={
                'error_type': 'RedisConnectionError',
                'ex': traceback.format_exc()
            })
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
            tweet_dict['traptor']['created_at_iso'] = self._tweet_time_to_iso(tweet_dict['created_at'])

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

        :return dict enriched_data: tweet dict with additional enrichments
        :return dict tweet: non-tweet message with no additional enrichments
        """
        enriched_data = dict()

        if self._message_is_limit_message(tweet):
            # Increment counter
            dd_monitoring.increment('limit_message_received')
            # Send DD the limit message value
            limit_count = tweet.get('limit').get(self.traptor_type, None)
            dd_monitoring.gauge('limit_message_count', limit_count, [])
            # Store the limit count in Redis
            self._increment_limit_message_counter(limit_count=limit_count)
        elif self._message_is_tweet(tweet):
            try:
                # Add the initial traptor fields
                tweet = self._create_traptor_obj(tweet)

                # Add the created_at_iso field
                tweet = self._add_iso_created_at(tweet)

                # Add the rule information
                enriched_data = self._find_rule_matches(tweet)

                # Update the matched rule stats
                if self.traptor_type != 'locations' and self.enable_stats_collection ==\
                        'true':
                    self._increment_rule_counter(enriched_data)
            except Exception as e:
                self.logger.error("Failed to enrich tweet, skipping enhancement", {
                    "tweet": json.dumps(tweet),
                    "ex"   : traceback.format_exc()
                })

                # an error occurred while processing the tweet. If some information was
                # set in the dictionary when calling _find_rule_matches, clear it out
                # because it is likely invalid...
                enriched_data = {}

        else:
            self.logger.info("Twitter message is not a tweet", extra={
                'twitter_message': tweet
            })

        dd_monitoring.increment('tweet_process_success')

        if enriched_data:
            return enriched_data
        else:
            return tweet

    def _check_redis_pubsub_for_restart(self):
        """
        Subscribe to Redis PubSub and restart if necessary.

        Check the Redis PubSub channel and restart Traptor if a message for
        this Traptor is found.
        """
        self.logger.info("Subscribing to the Traptor notification PubSub")
        self.logger.debug("restart_flag = {}".format(self.restart_flag))

        pubsub_check_interval = float(os.getenv('PUBSUB_CHECK_INTERVAL', 1))

        p = self.pubsub_conn.pubsub()
        p.subscribe(self.traptor_notify_channel)

        while True:
            time.sleep(pubsub_check_interval)
            m = p.get_message()
            if m is not None:
                data = str(m['data'])
                t = data.split(':')
                self.logger.debug("PubSub Message: {}".format(t))
                if t[0] == self.traptor_type and t[1] == str(self.traptor_id):
                    # Log the action and restart
                    self.restart_flag = True
                    self.logger.debug("Redis PubSub message found. Setting restart flag to True.")
                    dd_monitoring.increment('restart_message_received')

    @retry(wait=wait_exponential(multiplier=1, max=10),
           stop=stop_after_attempt(3),
           reraise=True,
           retry=retry_if_exception_type(redis.ConnectionError)
           )
    def _add_heartbeat_message_to_redis(self,
                                        heartbeat_conn):
        """Add a heartbeat message to Redis."""
        time_to_live = 5
        now = datetime.now().strftime("%Y%M%d%H%M%S")
        key_to_add = "{}:{}:{}".format(self.traptor_type,
                                       self.traptor_id,
                                       now)
        message = "alive"

        dd_monitoring.increment('heartbeat_message_sent_success')
        return heartbeat_conn.setex(key_to_add, time_to_live, message)

    def _send_heartbeat_message(self):
        """Add an expiring key to Redis as a heartbeat on a timed basis."""
        self.logger.info("Starting the heartbeat")
        hb_interval = 5

        # while Traptor is running, add a heartbeat message every 5 seconds
        while True:
            try:
                self._add_heartbeat_message_to_redis(self.heartbeat_conn)
            except Exception:
                self.logger.error("Caught exception while adding the heartbeat message to Redis", extra={
                    'error_type': 'RedisConnectionError',
                    'ex': traceback.format_exc()
                })
                dd_monitoring.increment('heartbeat_message_sent_failure',
                                        tags=['error_type:redis_connection_error'])
                raise

            time.sleep(hb_interval)

    @retry(wait=wait_exponential(multiplier=1, max=10),
           stop=stop_after_attempt(3),
           reraise=True,
           retry=retry_if_exception_type(KafkaUnavailableError)
           )
    def _send_enriched_data_to_kafka(self, tweet, enriched_data):
        """"
        Send the enriched data to Kafka

        :param tweet: the original tweet
        :param enriched_data: the enriched data to send
        """
        self.logger.info("Attempting to send tweet to kafka", extra={
            'tweet_id': tweet.get('id_str', None)
        })
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
        for item in self.birdy_stream._stream_iter():
            if item:
                try:
                    tweet = json.loads(item)
                except:
                    self.logger.error("Caught exception while json loading the Twitter message", extra={
                        'ex': traceback.format_exc()
                    })
                    dd_monitoring.increment('traptor_error_occurred',
                                            tags=['error_type:json_loads_error'])
                else:
                    enriched_data = self._enrich_tweet(tweet)

                    if self.kafka_enabled == 'true':

                        try:
                            self._send_enriched_data_to_kafka(tweet, enriched_data)
                        except:
                            self.logger.error("Caught exception adding Twitter message to Kafka", extra={
                                'ex': traceback.format_exc()
                            })
                            dd_monitoring.increment('tweet_to_kafka_failure',
                                                    tags=['error_type:kafka'])
                    else:
                        self.logger.debug(json.dumps(enriched_data, indent=2))

            if self.restart_flag:
                self.logger.info("Restart flag is true; restarting myself")
                break

    def _wait_for_rules(self):
        """Wait for the Redis rules to appear"""
        # Get the list of rules from Redis
        self.redis_rules = [rule for rule in self._get_redis_rules()]

        if len(self.redis_rules) == 0:
            self.logger.info("Waiting for rules")

        # If there are no rules assigned to this Traptor, simma down and wait a minute
        while len(self.redis_rules) == 0:
            self.logger.debug("No Redis rules assigned; Sleeping for 60 seconds")
            time.sleep(self.rule_check_interval)
            self.redis_rules = [rule for rule in self._get_redis_rules()]

    def run(self):
        """ Run method for running a traptor instance.

            It sets up the logging, connections, grabs the rules from redis,
            and starts writing data to kafka if enabled.
        """
        # Setup connections and logging
        self._setup()

        # Create the thread for the pubsub restart check
        ps_check = threading.Thread(group=None,
                                    target=self._check_redis_pubsub_for_restart
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
            self.logger.debug("Twitter rules: {}".format(self.twitter_rules.encode('utf-8')))

            # Make the rule and limit message counters
            if self.traptor_type != 'locations':
                if self.enable_stats_collection == 'true':
                    self._make_rule_counters()
                self._make_limit_message_counter()

            if not self.test:
                self._create_birdy_stream()

            if self.traptor_type == 'locations':
                self.locations_rule = self._get_locations_traptor_rule()

            self.restart_flag = False

            try:
                # Start collecting data
                self._main_loop()
            except ChunkedEncodingError as e:
                self.logger.error("Ran into a ChunkedEncodingError while processing "
                                  "tweets. Restarting Traptor from top of main process "
                                  "loop", {
                    'ex' : traceback.format_exc()
                })




def main():
    """ Command line interface to run a traptor instance.

        Can pass it flags for debug levels and also --stdout mode, which means
        it will not write to kafka but stdout instread.
    """

    # Redis connections
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_db = int(os.getenv('REDIS_DB', 5))

    redis_conn = redis.StrictRedis(host=redis_host,
                             port=redis_port,
                             db=redis_db,
                             decode_responses=True)

    # Redis pubsub connection
    pubsub_conn = redis.StrictRedis(host=redis_host,
                              port=redis_port,
                              db=redis_db)

    # Redis heartbeat connection
    heartbeat_conn = redis.StrictRedis(host=redis_host,
                                 port=redis_port,
                                 db=redis_db)

    # Twitter api keys
    api_keys = {
        'CONSUMER_KEY': os.getenv('CONSUMER_KEY'),
        'CONSUMER_SECRET': os.getenv('CONSUMER_SECRET'),
        'ACCESS_TOKEN': os.getenv('ACCESS_TOKEN'),
        'ACCESS_TOKEN_SECRET': os.getenv('ACCESS_TOKEN_SECRET')
    }


    # Create the traptor instance
    traptor_instance = Traptor(redis_conn=redis_conn,
                               pubsub_conn=pubsub_conn,
                               heartbeat_conn=heartbeat_conn,
                               traptor_notify_channel=os.getenv('REDIS_PUBSUB_CHANNEL', 'traptor-notify'),
                               rule_check_interval=int(os.getenv('RULE_CHECK_INTERVAL', 60)),
                               traptor_type=os.getenv('TRAPTOR_TYPE', 'track'),
                               traptor_id=int(os.getenv('TRAPTOR_ID', 0)),
                               apikeys=api_keys,
                               kafka_enabled=os.getenv('KAFKA_ENABLED', 'true'),
                               kafka_hosts=os.getenv('KAFKA_HOSTS', 'localhost:9092'),
                               kafka_topic=os.getenv('KAFKA_TOPIC', 'traptor'),
                               use_sentry=os.getenv('USE_SENTRY', 'false'),
                               sentry_url=os.getenv('SENTRY_URL', None),
                               test=False,
                               enable_stats_collection=os.getenv('ENABLE_STATS_COLLECTION', 'true')
                               )

    # Logger for this main function. The traptor has it's own logger

    traptor_name = 'traptor-{}-{}'.format(os.getenv('TRAPTOR_TYPE', 'track'),
                                          os.getenv('TRAPTOR_ID', 0))
    logger = LogFactory.get_instance(name=traptor_name,
                json=os.getenv('LOG_JSON', settings.LOG_JSON) == 'True',
                stdout=os.getenv('LOG_STDOUT', settings.LOG_STDOUT) == 'True',
                level=os.getenv('LOG_LEVEL', settings.LOG_LEVEL),
                dir=os.getenv('LOG_DIR', settings.LOG_DIR),
                file=os.getenv('LOG_FILE', settings.LOG_FILE))

    if settings.DW_ENABLED:
        dw_config(settings.DW_CONFIG)
        logger.register_callback('>=INFO', dw_callback)

    # Wait until all the other containers are up and going...
    time.sleep(30)

    # Run the traptor instance
    try:
        logger.info('Starting Traptor')
        logger.debug("Traptor info: {}".format(traptor_instance.__repr__()))
        traptor_instance.run()
    except Exception as e:
        if os.getenv('USE_SENTRY') == 'true':
            client = Client(os.getenv('SENTRY_URL'))
            client.captureException()

        logger.error("Caught exception when starting Traptor", extra={
            'ex': traceback.format_exc()
        })

        dd_monitoring.increment('traptor_error_occurred',
                                tags=['error_type:traptor_start'])
        raise e

if __name__ == '__main__':
    from raven import Client

    sys.exit(main())
