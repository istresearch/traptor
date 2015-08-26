#!/usr/bin/env python

from birdy.twitter import StreamClient
import json
import redis
import logging
from kafka import SimpleProducer, KafkaClient

import logging

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

def rscanit():
    twids = []
    for idx, key in enumerate(r.sscan_iter('twitter_ids')):
        if idx < 5000:
            twids.append(key)
            logger.debug('{0}: {1}'.format(idx, key))
    return twids

def rpopit():
    twids = []
    for x in xrange(1,5000):
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
    client = KafkaClient(hosts="k01.istresearch.com:9092,\
                         k02.istresearch.com:9092,k03.istresearch.com:9092")
    producer = SimpleProducer(client)
    return producer

def run():
    twids_str = ','.join(zscanit())
    client = StreamClient("yJYAUb9vOhISx2U4Qt5QsAKcE",
                        "FqlFbqtzBJQRXqXOA839sgWre3sWmQ3HeOFj8qEDnyCyebNm9Y",
                        "3432988792-xuqAvsVTKrjQb8fZNDz1h2FgBuo9ha145yHeswB",
                        "IPJ6Hg5JhxtH2MSC6TzKHJ0wTsOKFKwBFGP3OAeSNkM4I")

    resource = client.stream.statuses.filter.post(follow=twids_str)

    topic_name = 'traptor.prod-darpa1'
    producer = kafka_producer()

    for data in resource.stream():
        logger.info(json.dumps(data.get('text')))
        logger.debug(json.dumps(data))
        producer.send_messages(topic_name, json.dumps(data))


# print json.dumps(next(resource.stream()), indent=2)

if __name__ == '__main__':
    twids = zscanit()
