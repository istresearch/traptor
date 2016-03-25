#!/usr/bin/env python
import json
import re
import sys
import time
import dateutil.parser as parser

from redis import StrictRedis, ConnectionError
from kafka import SimpleProducer, KafkaClient
from kafka.common import (NotLeaderForPartitionError, KafkaUnavailableError)
from birdy.twitter import StreamClient, TwitterApiError
import click
from flatdict import FlatDict

import threading

from scutils.log_factory import LogFactory


# Override the default JSONobject
class MyBirdyClient(StreamClient):
    @staticmethod
    def get_json_object_hook(data):
        return data


class Traptor(object):

    def __init__(self,
                 redis_conn,
                 pubsub_conn,
                 traptor_type,
                 apikeys,
                 traptor_id=0,
                 kafka_hosts='localhost:9092',
                 kafka_topic='traptor',
                 kafka_enabled=True,
                 log_level='INFO',
                 test=False,
                 traptor_notify_channel='traptor-notify'
                 ):
        """
        Traptor base class.

        :param dict apikeys: dictionary of API keys for traptor instnace.  See
                             settings.py for details.
        :param str traptor_type: follow, track, or geo.
        :param int traptor_id: numerical ID of traptor instance.
        :param str kafka_hosts: kafka hosts to connect to.
        :param str kafka_topic: name of the kafka topic to write to.
        :param str redis_conn: redis connection to use.
        :param bool kafka_enabled: write to kafka or just log to something else.
        :param str log_level: log level of the traptor logger instance.
        :param bool test: True for traptor test instance.
        :param str traptor_notify_channel: name of the Traptor PubSub channel to subscribe to
        :param str pubsub_conn: redis pubsub connection to use

        """
        self.apikeys = apikeys
        self.traptor_type = traptor_type
        self.traptor_id = traptor_id
        self.kafka_hosts = kafka_hosts
        self.kafka_topic = kafka_topic
        self.redis_conn = redis_conn
        self.kafka_enabled = kafka_enabled
        self.log_level = log_level
        self.test = test
        self.traptor_notify_channel = traptor_notify_channel
        self.pubsub_conn = pubsub_conn

    def __repr__(self):
        return 'Traptor({}, {}, {}, {}, {}, {}, {}, {}, {}, {} ,{})'.format(
            self.apikeys,
            self.traptor_type,
            self.traptor_id,
            self.kafka_hosts,
            self.kafka_topic,
            self.redis_conn,
            self.kafka_enabled,
            self.log_level,
            self.test,
            self.traptor_notify_channel,
            self.pubsub_conn
        )

    def _setup_birdy(self):
        """ Set up a birdy twitter stream.
            If there is a TwitterApiError it will exit with status code 3.
            This was done to prevent services like supervisor from automatically
            restart the process causing the twitter API to get locked out.

            Creates ``self.birdy_conn``.
        """

        # Set up a birdy twitter streaming client
        self.logger.info('Setting up birdy connection...')
        self.birdy_conn = MyBirdyClient(
                                        self.apikeys['CONSUMER_KEY'],
                                        self.apikeys['CONSUMER_SECRET'],
                                        self.apikeys['ACCESS_TOKEN'],
                                        self.apikeys['ACCESS_TOKEN_SECRET']
                                        )

    def _setup_kafka(self):
        """ Set up a Kafka connection.

            Creates ``self.kafka_conn`` if it can reach the kafka brokers.
        """
        if self.kafka_enabled:
            self.logger.info('Setting up kafka connection...')
            self.kafka_conn = KafkaClient(hosts=self.kafka_hosts)
        else:
            self.logger.info('Skipping kafka connection setup')
            self.kafka_conn = None

    def _setup(self):
        """
        Load everything up. Note that any arg here will override both
        default and custom settings.
        """

        # Set up logging
        self.logger = LogFactory.get_instance(name='traptor',
                                              level=self.log_level)

        # Set the restart_flag to False
        self.restart_flag = False

        # Set up required connections
        self._setup_kafka()
        self._setup_birdy()


    def _create_kafka_producer(self, kafka_topic):
        """ Create a kafka producer.
            If it cannot find one it will exit with error code 3.

            Creates ``self.kafka_producer``.
        """
        if self.kafka_conn:
            try:
                self.logger.debug('Creating kafka producer for "{}"...'.format(self.kafka_topic))
                self.kafka_producer = SimpleProducer(self.kafka_conn)
            except KafkaUnavailableError as e:
                self.logger.critical(e)
                sys.exit(3)
            try:
                self.logger.debug('Ensuring the "{}" kafka topic exists'.format(self.kafka_topic))
                self.kafka_conn.ensure_topic_exists(self.kafka_topic)
            except:
                raise
        else:
            self.kafka_producer = None

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
                self.logger.info('Creating birdy "follow" stream')
                self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(follow=self.twitter_rules)
            except TwitterApiError as e:
                self.logger.critical(e)
                sys.exit(3)
        elif self.traptor_type == 'track':
            # Try to set up a twitter stream using twitter term list
            try:
                self.logger.info('Creating birdy "track" stream')
                self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(track=self.twitter_rules)
            except TwitterApiError as e:
                self.logger.critical(e)
                sys.exit(3)
        elif self.traptor_type == 'locations':
            # Try to set up a twitter stream using twitter term list
            try:
                self.logger.info('Creating birdy "locations" stream')
                self.birdy_stream = self.birdy_conn.stream.statuses.filter.post(locations=self.twitter_rules)
            except TwitterApiError as e:
                self.logger.critical(e)
                sys.exit(3)
        else:
            self.logger.critical('That traptor type has not been implemented')
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

    def _add_rule_tag_and_value_to_tweet(self, tweet_dict, search_str, rule_tag, rule_value):

        for k, v in FlatDict(tweet_dict).iteritems():
            if isinstance(v, unicode) and search_str.lower() in v.lower():
                tweet_dict['traptor']['rule_tag'] = rule_tag
                tweet_dict['traptor']['rule_value'] = rule_value
            else:
                self.logger.warning('Could not find rule match for {}'.format(
                                                    tweet_dict.get('id_str')))

        return tweet_dict

    def _find_rule_matches(self, tweet_dict):
        """ Find which rule the tweet matched.  This code only expects there to
            be one match.  If there is more than one, it will use the last one
            it finds since the first match will be overwritten.

            :param dict tweet_dict: The dictionary twitter object.
            :returns: a ``dict`` with the augmented data fields.
        """
        new_dict = self._create_traptor_obj(tweet_dict)
        self.logger.debug('Finding tweet rule matches')

        for rule in self.redis_rules:
            search_str = rule['value']
            # self.logger.debug("Search string used for the rule match: {}".format(search_str.encode('utf-8')))
            if re.search(',', search_str):
                for s in search_str.split(','):
                    new_dict = self._add_rule_tag_and_value_to_tweet(new_dict,
                                                                     s,
                                                                     rule['tag'],
                                                                     rule['value'])
            else:
                search_str = rule['value'].split()[0]
                for i in new_dict.keys():
                    new_dict = self._add_rule_tag_and_value_to_tweet(new_dict,
                                                                     search_str,
                                                                     rule['tag'],
                                                                     rule['value'])
            # self.logger.debug('Rule matched - tag:{}, value:{}'.format(rule['tag'],
            #                                                            rule['value'].encode('utf-8')))
        return new_dict

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
            rule_max = 25
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

    @staticmethod
    def _tweet_time_to_iso(tweet_time):
        """ Convert tweet time into ISO time format.

            :returns: A ``str`` of the ISO formated time.
        """
        return parser.parse(tweet_time).isoformat()

    def _create_traptor_obj(self, tweet_dict):
        if 'traptor' not in tweet_dict:
            tweet_dict['traptor'] = {}

        return tweet_dict

    def _fix_tweet_object(self, tweet_dict):
        """ Do any pre-processing to raw tweet data.

            :param dict tweet_dict: A tweet dictionary object.
            :returns: A ``dict`` with a new 'created_at_iso field.
        """
        new_dict = self._create_traptor_obj(tweet_dict)
        if new_dict.get('created_at'):

            new_dict['traptor']['created_at_iso'] = self._tweet_time_to_iso(
                                                    new_dict['created_at'])
            # self.logger.debug('Fixed tweet object: \n {}'.format(
            #                   json.dumps(new_dict, indent=2)))
        return new_dict

    def _check_redis_pubsub_for_restart(self):
        """
        Subscribe to Redis PubSub and restart if necessary.

        Check the Redis PubSub channel and restart Traptor if a message for
        this Traptor is found.
        """
        self.logger.info("Subscribing to the Traptor notification PubSub.")
        self.logger.debug("restart_flag = {}".format(self.restart_flag))
        p = self.pubsub_conn.pubsub()
        p.subscribe(self.traptor_notify_channel)

        while self.restart_flag is not True:
            m = p.get_message()
            if m is not None:
                data = str(m['data'])
                t = data.split(':')
                self.logger.debug("PubSub Message: {}".format(t))
                if t[0] == self.traptor_type and t[1] == str(self.traptor_id):
                    # Log the action and restart
                    self.restart_flag = True
                    self.logger.debug("Redis PubSub message found. \
                                      Setting restart flag to True.")

    def _main_loop(self):
        """
        Main loop for iterating through the twitter data.

        This method iterates through the birdy stream, does any
        pre-processing, and adds enrichments to the data.  If kafka is
        enabled it will write to the kafka topic defined when instantiating
        the Traptor class.
        """
        # Iterate through the twitter results
        for item in self.birdy_stream._stream_iter():
            if item:
                try:
                    _data = json.loads(item)
                except:
                    pass
                else:
                    # self.logger.debug('Raw Tweet Data: \n {0}'.format(
                    #                   json.dumps(_data, indent=2)))

                    # Do tweet data pre-processing
                    data = self._fix_tweet_object(_data)

                    # Do any data enrichment on the base tweet data
                    enriched_data = self._find_rule_matches(data)

                    # Stdout data output for Traptor.
                    print json.dumps(enriched_data, indent=2)

                    if self.kafka_enabled:
                        self.kafka_producer.send_messages(self.kafka_topic,
                                                          json.dumps(enriched_data))

            if self.restart_flag:
                self.logger.info("Reset flag is true; restarting myself.")
                break

    def run(self):
        """ Run method for running a traptor instance.

            It sets up the logging, connections, grabs the rules from redis,
            and starts writing data to kafka if enabled.
        """
        # Setup connections and logging
        self._setup()

        ps_check = threading.Thread(group=None,
                                    target=self._check_redis_pubsub_for_restart
                                    )
        ps_check.setDaemon(True)
        ps_check.start()

        while True:
            # Grab a list of {tag:, value:} rules
            self.redis_rules = [rule for rule in self._get_redis_rules()]
            self.logger.debug("Redis rules: {}".format(self.redis_rules))

            # Concatenate all of the rule['value'] fields
            self.twitter_rules = self._make_twitter_rules(self.redis_rules)
            self.logger.debug("Twitter rules: {}".format(self.twitter_rules.encode('utf-8')))

            if self.kafka_enabled:
                self._create_kafka_producer(self.kafka_topic)

            if not self.test:
                self._create_birdy_stream()

            self.restart_flag = False

            # Start collecting data
            self._main_loop()

@click.command()
@click.option('--test', is_flag=True)
@click.option('--info', is_flag=True)
@click.option('--debug', is_flag=True)
@click.option('--delay', default=1)
@click.option('--id')
@click.option('--type')
@click.option('--key', default=0)
def main(test, info, debug, delay, id, type, key):
    """ Command line interface to run a traptor instance.

        Can pass it flags for debug levels and also --test mode, which means
        it will not write to kafka but stdout instread.
    """

    kafka_enabled = False if test else True
    if debug:
        log_level = 'DEBUG'
    elif info:
        log_level = 'INFO'
    else:
        log_level = 'CRITICAL'

    traptor_id = id if id else TRAPTOR_ID
    traptor_type = type if type else TRAPTOR_TYPE

    redis_conn = StrictRedis(host=REDIS_HOST,
                             port=REDIS_PORT,
                             db=REDIS_DB,
                             decode_responses=True)

    pubsub_conn = StrictRedis(host=REDIS_HOST,
                              port=REDIS_PORT,
                              db=REDIS_DB)

    traptor_instance = Traptor(apikeys=APIKEYS[key],
                               traptor_type=traptor_type,
                               traptor_id=traptor_id,
                               kafka_hosts=KAFKA_HOSTS,
                               kafka_topic=KAFKA_TOPIC,
                               redis_conn=redis_conn,
                               traptor_notify_channel=REDIS_PUBSUB_CHANNEL,
                               pubsub_conn=pubsub_conn,
                               kafka_enabled=kafka_enabled,
                               log_level=log_level,
                               test=False,
                               )

    # Don't connect to the Twitter API too fast
    time.sleep(delay)
    # Run the traptor instance and start collecting data
    traptor_instance.run()

if __name__ == '__main__':
    from settings import (KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID,
                          TRAPTOR_TYPE, REDIS_HOST, REDIS_PORT, REDIS_DB,
                          REDIS_PUBSUB_CHANNEL)
    sys.exit(main())
