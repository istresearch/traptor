#!/usr/bin/env python
import json
import logging
import sys
import time

from redis import StrictRedis, ConnectionError
from kafka import SimpleProducer, KafkaClient
from kafka.common import (NotLeaderForPartitionError, KafkaUnavailableError)
from birdy.twitter import StreamClient, TwitterApiError

from settings import (KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID,
                      TRAPTOR_TYPE, REDIS_HOST)

# logging.basicConfig(level=logging.CRITICAL)

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-4s %(levelname)-4s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def get_redis_twitter_ids(traptor_type=TRAPTOR_TYPE, traptor_id=TRAPTOR_ID,
                          redis_host=REDIS_HOST):
    """ Return a list of twitter ids from the service server.  This function
        expects that the redis keys are set up like follows:

        traptor-<traptor_type>:<crawler_num>

        For example,

        traptor-follow:0
        traptor-follow:1

        In the case of the 'follow' twitter streaming, each traptor may only
        follow 5000 twitter ids, as per the Twitter API.
    """
    # This line is lazy, nothing touches Redis until a command is issued
    r = StrictRedis(host=redis_host, port=6379, db=0)

    twids = []
    redis_key = 'traptor-{0}:{1}'.format(traptor_type, traptor_id)
    try:
        for idx, key in enumerate(r.sscan_iter(redis_key)):
            if idx < 5000:
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
    # Check traptor_type
    if traptor_type == 'follow':
        # Set up a birdy twitter streaming client
        client = StreamClient(
                              APIKEYS['CONSUMER_KEY'],
                              APIKEYS['CONSUMER_SECRET'],
                              APIKEYS['ACCESS_TOKEN'],
                              APIKEYS['ACCESS_TOKEN_SECRET']
                          )
        # Try to set up a twitter stream using twitter id list
        try:
            resource = client.stream.statuses.filter.post(follow=rules)
            return resource
        except TwitterApiError as e:
            logger.critical(e)
            sys.exit(3)
    else:
        sys.exit('That traptor type has not been implemented yet')


def run():
    # Grab a list of twitter ids from the get_redis_twitter_ids function
    twids_str = ','.join(get_redis_twitter_ids())

    # Set up Kafka producer
    producer = create_kafka_producer()

    # Set up a birdy streaming client
    birdyclient = create_birdy_stream(twids_str)

    # Iterate through the twitter results
    for data in birdyclient.stream():
        logger.info(json.dumps(data.get('text')))
        logger.debug(json.dumps(data))
        try:
            producer.send_messages(KAFKA_TOPIC, json.dumps(data))
        except:
            logger.error('Could not write to kafka topic {0}'.format(
                         KAFKA_TOPIC))
            sys.exit(3)


def main():
    run()

if __name__ == '__main__':
    main()
