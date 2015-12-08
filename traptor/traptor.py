#!/usr/bin/env python
import json
import logging
import re
import sys
import time
import dateutil.parser as parser

from redis import StrictRedis, ConnectionError
from kafka import SimpleProducer, KafkaClient
from kafka.common import (NotLeaderForPartitionError, KafkaUnavailableError)
from birdy.twitter import StreamClient, TwitterApiError
import click

from scutils.log_factory import LogFactory

from settings import (KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID,
                      TRAPTOR_TYPE, REDIS_HOST, REDIS_PORT, REDIS_DB,
                      LOG_LEVEL)


# Override the default JSONobject
class MyBirdyClient(StreamClient):
    @staticmethod
    def get_json_object_hook(data):
        return data


class Traptor(object):

    def __init__(self,
                 apikeys=None,
                 traptor_type='track',
                 traptor_id=0,
                 kafka_hosts='localhost:9092',
                 kafka_topic='traptor',
                 redis_host='localhost',
                 redis_port=6379,
                 redis_db=0,
                 kafka_enabled=True,
                 log_level='INFO'
                 ):
        """
        Traptor base class.

        :param int traptor_id: numerical ID of traptor instance
        :param str traptor_type: follow, track, or geo
        """
        self.apikeys = apikeys
        self.traptor_type = traptor_type
        self.traptor_id = traptor_id
        self.kafka_hosts = kafka_hosts
        self.kafka_topic = kafka_topic
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.kafka_enabled = kafka_enabled
        self.log_level = log_level

    def __repr__(self):
        return 'Traptor({}, {}, {}, {}, {}, {}, {}, {}, {}, {})'.format(
            self.apikeys,
            self.traptor_type,
            self.traptor_id,
            self.kafka_hosts,
            self.kafka_topic,
            self.redis_host,
            self.redis_port,
            self.redis_db,
            self.kafka_enabled,
            self.log_level
        )

    def setup(self, level='INFO', log_file=None, json=None):
        """
        Load everything up. Note that any arg here will override both
        default and custom settings

        :param str level: the log level
        :param bool log_file: boolean t/f whether to log to a file, else stdout
        :param bool json: boolean t/f whether to write the logs in json
        """

        # Set up logging
        # my_level = level if level else self.settings['LOG_LEVEL']
        self.logger = LogFactory.get_instance(name='traptor', level=level)

        # Set up all connections
        self._setup_redis(self)
        self._setup_kafka(self)
        self._setup_birdy(self)

    def _setup_birdy(self):
        """ Set up a birdy twitter stream.
            If there is a TwitterApiError it will exit with status code 3.
            This was done to prevent services like supervisor from automatically
            restart the process causing the twitter API to get locked out.
        """

        # Set up a birdy twitter streaming client
        self.birdy_conn = MyBirdyClient(
                                        self.apikeys['CONSUMER_KEY'],
                                        self.apikeys['CONSUMER_SECRET'],
                                        self.apikeys['ACCESS_TOKEN'],
                                        self.apikeys['ACCESS_TOKEN_SECRET']
                                        )

    def _setup_kafka(self):
        """ Set up a Kafka connection. """
        if self.kafka_enabled:
            self.kafka_conn = KafkaClient(hosts=self.kafka_hosts)
        else:
            self.kafka_conn = None

    def _setup_redis(self):
        """Set up a Redis connection
            This line is lazy, nothing touches Redis until a command is issued.
        """
        self.redis_conn = StrictRedis(host=self.redis_host,
                                      port=self.redis_port,
                                      db=self.redis_db)

    def _create_kafka_producer(self, kafka_topic):
        """ Create a kafka producer.
            If it cannot find one it will exit with error code 3.

            Creates self.kafka_producer
        """
        try:
            self.kafka_producer = SimpleProducer(self.kafka_conn)
        except KafkaUnavailableError as e:
            self.logger.critical(e)
            sys.exit(3)
        try:
            self.kafka_conn.ensure_topic_exists(self.kafka_topic)
        except:
            raise

    def _create_birdy_stream(self):
        """ Create a birdy twitter stream.
            If there is a TwitterApiError it will exit with status code 3.
            This was done to prevent services like supervisor from automatically
            restart the process causing the twitter API to get locked out.

            Creates self.birdy_stream
        """

        if self.traptor_type == 'follow':
            # Try to set up a twitter stream using twitter id list
            try:
                self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(follow=self.twitter_rules)
            except TwitterApiError as e:
                self.logger.critical(e)
                sys.exit(3)
        elif self.traptor_type == 'track':
            # Try to set up a twitter stream using twitter term list
            try:
                self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(track=self.twitter_rules)
            except TwitterApiError as e:
                self.logger.critical(e)
                sys.exit(3)
        else:
            self.logger.critical('That traptor type has not been implemented yet')
            sys.exit(3)

    def write_to_kafka(self):
        pass

    def write_to_stdout(self):
        pass

    @staticmethod
    def _make_twitter_rules(rules):
        rules_str = ','.join([rule['value'] for rule in rules])
        return rules_str

    def _find_rule_matches(self, data):
        """ Find which rule the tweet matched.  This code only expects there to
            be one match.  If there is more than one, it will use the last one
            it finds since the first match will be overwritten.
        """
        for rule in self.redis_rules:
            search_str = rule['value'].split()[0]
            if re.search(search_str, json.dumps(data)):
                data['rule_tag'] = rule['tag']
                data['rule_value'] = rule['value']

        self.logger.info('utf-8 Text: {0}'.format(
                         data.get('text').encode('utf-8')))
        self.logger.info('Rule matched - tag:{}, value:{}'.format(
                    data.get('rule_tag'), data.get('rule_value')))
        self.logger.debug('Cleaned Data: {0}'.format(json.dumps(data)))

        return data

    def _get_redis_rules(self):
        """ Return a list of twitter ids from the service server.  This function
            expects that the redis keys are set up like follows:

            traptor-<traptor_type>:<crawler_num>

            For example,

            traptor-follow:0
            traptor-follow:1

            traptor-track:0
            traptor-track:1

            For 'follow' twitter streaming, each traptor may only
            follow 5000 twitter ids, as per the Twitter API.

            For 'track' twitter stream, each traptor may only
            track 400 keywords, as per the Twitter API.

            :returns: Yields a traptor rule from redis.
        """
        # Set up API limitation checks
        if self.traptor_type == 'follow':
            rule_max = 5000
        elif self.traptor_type == 'track':
            rule_max = 400
        else:
            self.logger.error('traptor_type of {0} is not supported'.format(
                         self.traptor_type))
            raise(NotImplementedError)

        # for rule in xrange(rule_max):
        redis_key = 'traptor-{0}:{1}'.format(self.traptor_type,
                                             self.traptor_id)
        match = ':'.join([redis_key, '*'])
        try:
            for idx, hashname in enumerate(self.redis_conn.scan_iter(match=match)):
                if idx < rule_max:
                    redis_rule = self.redis_conn.hgetall(hashname)
                    yield redis_rule
                    self.logger.debug('Index: {0}, Redis_rule: {1}'.format(
                                      idx, redis_rule))
        except ConnectionError as e:
            self.logger.critical(e)
            sys.exit(3)  # Special error code to track known failures

        return twids

    @staticmethod
    def _tweet_time_to_iso(tweet_time):
        return parser.parse(tweet_time).isoformat()

    def _clean_tweet_data(self, tweet_dict):
        """ Do any pre-processing to raw tweet data before passing on
            to Kafka
        """

        if tweet_dict.get('created_at'):
            tweet_dict['created_at'] = self._tweet_time_to_iso(
                                                    tweet_dict['created_at'])
        return tweet_dict

    def _main_loop(self):
        # Iterate through the twitter results
        for _data in self.birdy_stream.stream():
            self.logger.debug('Raw Data: {0}'.format(json.dumps(_data)))

            # Do tweet data pre-processing
            data = self._clean_tweet_data(_data)

            # Do any data enrichment on the base tweet data
            enriched_data = self._find_rule_matches(data)
            self.logger.info('Text: {}'.format(
                             json.dumps(enriched_data['text'])))

            if self.kafka_enabled:
                self.kafka_producer.send_messages(self.kafka_topic,
                                                  json.dumps(data))

    def run(self):
        # Set up logging
        self.logger = LogFactory.get_instance(name='traptor',
                                              level=self.log_level)

        # Set up required connections
        self._setup_redis()
        self._setup_birdy()
        if self.kafka_enabled:
            self._setup_kafka()

        # Grab a list of {tag:, value:} rules
        self.redis_rules = [rule for rule in self._get_redis_rules()]

        # Concatenate all of the rule['value'] fields
        self.twitter_rules = self._make_twitter_rules(self.redis_rules)

        # Create bridy and kafka connections
        self._create_birdy_stream()
        if self.kafka_enabled:
            self._create_kafka_producer(self.kafka_topic)

        # Start collecting data
        self._main_loop()


@click.command()
@click.option('--test', is_flag=True)
@click.option('--info', is_flag=True)
@click.option('--debug', is_flag=True)
def main(test, info, debug):
    kafka_enabled = False if test else True
    if debug:
        log_level = 'DEBUG'
    elif info:
        log_level = 'INFO'
    else:
        log_level = 'CRITICAL'

    traptor_instance = Traptor(apikeys=APIKEYS,
                               traptor_type=TRAPTOR_TYPE,
                               traptor_id=TRAPTOR_ID,
                               kafka_hosts=KAFKA_HOSTS,
                               redis_host=REDIS_HOST,
                               redis_port=REDIS_PORT,
                               redis_db=REDIS_DB,
                               kafka_enabled=kafka_enabled,
                               log_level=log_level
                               )

    # Setup Logging, Redis, and Kafka connections
    # traptor_instance.setup(level=LOG_LEVEL)
    # Run the traptor instance and start collecting data
    traptor_instance.run()

if __name__ == '__main__':
    sys.exit(main())
