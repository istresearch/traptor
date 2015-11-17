#!/usr/bin/env python
import json
import logging
import sys
import time
import dateutil.parser as parser

from redis import StrictRedis, ConnectionError
from kafka import SimpleProducer, KafkaClient
from kafka.common import (NotLeaderForPartitionError, KafkaUnavailableError)
from birdy.twitter import StreamClient, TwitterApiError
import click

from settings import (KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID,
                      TRAPTOR_TYPE, REDIS_HOST)

# logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-4s %(levelname)-4s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# Override the default JSONobject
class MyClient(StreamClient):
    @staticmethod
    def get_json_object_hook(data):
        return data


def get_redis_twitter_rules(traptor_type=TRAPTOR_TYPE, traptor_id=TRAPTOR_ID,
                            redis_host=REDIS_HOST):
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
    """
    # Set up API limitation checks
    if traptor_type == 'follow':
        rule_max = 5000
    elif traptor_type == 'track':
        rule_max = 400
    else:
        logger.error('traptor_type of {0} is not supported'.format(
                     traptor_type))
        return list()

    # This line is lazy, nothing touches Redis until a command is issued
    r = StrictRedis(host=redis_host, port=6379, db=0)

    twids = []
    redis_key = 'traptor-{0}:{1}'.format(traptor_type, traptor_id)
    try:
        for idx, key in enumerate(r.sscan_iter(redis_key)):
            if idx < rule_max:
                twids.append(key)
                logger.debug('{0}: {1}'.format(idx, key))
    except ConnectionError as e:
        logger.critical(e)
        sys.exit(3)  # Special error code to track known failures
    return twids


def create_kafka_producer(kafka_hosts=KAFKA_HOSTS, kafka_topic=KAFKA_TOPIC):
    """ Create a kafka producer.
        If it cannot find one it will exit with error code 3.
    """
    try:
        client = KafkaClient(hosts=kafka_hosts)
        producer = SimpleProducer(client)
    except KafkaUnavailableError as e:
        logger.critical(e)
        sys.exit(3)
    try:
        client.ensure_topic_exists(kafka_topic)
    except:
        raise

    return producer


def create_birdy_stream(rules,
                        traptor_type=TRAPTOR_TYPE,
                        traptor_id=TRAPTOR_ID,
                        ):
    """ Set up a birdy twitter stream.
        If there is a TwitterApiError it will exit with status code 3.
        This was done to prevent services like supervisor from automatically
        restart the process causing the twitter API to get locked out.
    """

    # Set up a birdy twitter streaming client
    client = MyClient(
                          APIKEYS['CONSUMER_KEY'],
                          APIKEYS['CONSUMER_SECRET'],
                          APIKEYS['ACCESS_TOKEN'],
                          APIKEYS['ACCESS_TOKEN_SECRET']
                      )
    if traptor_type == 'follow':
        # Try to set up a twitter stream using twitter id list
        try:
            resource = client.stream.statuses.filter.post(follow=rules)
            return resource
        except TwitterApiError as e:
            logger.critical(e)
            sys.exit(3)
    elif traptor_type == 'track':
        # Try to set up a twitter stream using twitter term list
        try:
            resource = client.stream.statuses.filter.post(track=rules)
            return resource
        except TwitterApiError as e:
            logger.critical(e)
            sys.exit(3)
    else:
        logger.critical('That traptor type has not been implemented yet')
        sys.exit(3)


def clean_tweet_data(tweet_dict):
    """ Do any pre-processing to raw tweet data before passing on
        to Kafka
    """

    def tweet_time_to_iso(tweet_time):
        return parser.parse(tweet_time).isoformat()

    if tweet_dict.get('created_at'):
        tweet_dict['created_at'] = tweet_time_to_iso(tweet_dict['created_at'])
    return tweet_dict


@click.command()
@click.option('--test', is_flag=True)
def run(test):
    # Grab a list of twitter ids from the get_redis_twitter_rules function
    rules_str = ','.join(get_redis_twitter_rules())

    if not test:
        # Set up Kafka producer
        producer = create_kafka_producer()

        # Set up a birdy streaming client
        time.sleep(60)

    birdyclient = create_birdy_stream(rules_str)

    # Iterate through the twitter results
    for _data in birdyclient.stream():
        logger.info('utf-8 Text: {0}'.format(_data.get('text').encode('utf-8')))
        logger.debug('Raw Data: {0}'.format(json.dumps(_data)))

        # Do tweet data pre-processing
        data = clean_tweet_data(_data)
        logger.debug('Cleaned Data: {0}'.format(json.dumps(data)))

        if not test:
            # Send to Kafka
            producer.send_messages(KAFKA_TOPIC, json.dumps(data))


def main():
    run()

if __name__ == '__main__':
    run()
