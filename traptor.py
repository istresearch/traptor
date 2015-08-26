#!/usr/bin/env python

from birdy.twitter import StreamClient
import json
import redis
import logging

from kafka import SimpleProducer, KafkaClient
from settings import KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID, TRAPTOR_TYPE

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-4s %(levelname)-4s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# logging.basicConfig(level=logging.CRITICAL)
# logging.getLogger(__main__)

r = redis.StrictRedis(host='localhost', port=6379, db=0)


def sscanit(traptor_type, traptor_id):
    twids = []
    for idx, key in enumerate(r.sscan_iter(traptor_id)):
        if idx < 5000:
            twids.append(key)
            logger.debug('{0}: {1}'.format(idx, key))
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
    client = StreamClient(APIKEYS['CONSUMER_KEY'],
                          APIKEYS['CONSUMER_SECRET'],
                          APIKEYS['ACCESS_TOKEN'],
                          APIKEYS['ACCESS_TOKEN_SECRET']
                          )

    resource = client.stream.statuses.filter.post(follow=twids_str)

    topic_name = KAFKA_TOPIC
    producer = kafka_producer()

    for data in resource.stream():
        logger.info(json.dumps(data.get('text')))
        logger.debug(json.dumps(data))
        producer.send_messages(topic_name, json.dumps(data))


if __name__ == '__main__':
    twids = zscanit()
