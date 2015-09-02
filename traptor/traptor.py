#!/usr/bin/env python

from birdy.twitter import StreamClient
from birdy import twitter
import json
from redis import StrictRedis, ConnectionError
import logging
import sys

from kafka import SimpleProducer, KafkaClient
from settings import KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID, TRAPTOR_TYPE, REDIS_HOST

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-4s %(levelname)-4s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# logging.basicConfig(level=logging.CRITICAL)
# logging.getLogger(__main__)


def sscanit(traptor_type, traptor_id, redis_host=REDIS_HOST):
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
        # 3 is still an error code, but supervisor will not try to restart
        sys.exit(3)
    return twids


def rpopit():
    twids = []
    for x in xrange(1, 5000):
        if r.scard('twitter_ids'):
            twids.append(r.spop('twitter_ids'))
    return twids


def zscanit():
    twids = []
    for idx, key in enumerate(r.zscan_iter('twitter_ids')):
        if 5000 < idx < 10000:
            twids.append(key[0])
            logger.debug('{0}: {1}'.format(idx, key))
    return twids


def kafka_producer():
    client = KafkaClient(hosts=KAFKA_HOSTS)
    producer = SimpleProducer(client)
    return producer


def run(traptor_type=TRAPTOR_TYPE, traptor_id=TRAPTOR_ID):
    twids_str = ','.join(sscanit(traptor_type, traptor_id))
    logger.debug('Twitter ids: {0}'.format(twids_str))
    client = StreamClient(APIKEYS['CONSUMER_KEY'],
                          APIKEYS['CONSUMER_SECRET'],
                          APIKEYS['ACCESS_TOKEN'],
                          APIKEYS['ACCESS_TOKEN_SECRET']
                          )
    if traptor_type == 'follow':
        try:
            resource = client.stream.statuses.filter.post(follow=twids_str)
        except twitter.TwitterApiError as e:
            logger.critical(e)
            # 3 is still an error code, but supervisor will not try to restart
            sys.exit(3)
    elif traptor_type == 'track':
        sys.exit('track not implemented yet')
    else:
        sys.exit('that type has not been implemented or does not exist')

    topic_name = KAFKA_TOPIC
    producer = kafka_producer()

    for data in resource.stream():
        logger.info(json.dumps(data.get('text')))
        logger.debug(json.dumps(data))
        try:
            producer.send_messages(topic_name, json.dumps(data))
        except kafka.common.NotLeaderForPartitionError as e:
            logger.error(e)

def main():
    run()

if __name__ == '__main__':
    main()
