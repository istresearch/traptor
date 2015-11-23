# from nose.tools import assert_equal
from unittest import TestCase
from mock import MagicMock
from kafka import SimpleProducer
from redis import StrictRedis, ConnectionError

from scripts.rule_extract import CooperRules, RulesToRedis
from sample_rules import FOLLOW_RULES, TRACK_RULES
from traptor.traptor import (MyClient, get_redis_twitter_rules,
                             create_kafka_producer,
                             create_birdy_stream, clean_tweet_data, run
                             )


class TestTwitterRules(TestCase):
    """ Test that we can read the rules from Redis and return a twitter
        ruel string.
    """
    def setUp(self):

        self.fixed_follow_rules = CooperRules._fix_follow(FOLLOW_RULES)
        self.fixed_track_rules = CooperRules._fix_track(TRACK_RULES)

        # Need to test RulesToRedis class somewhere
        r = RulesToRedis()
        r.connect()
        r.redis_conn.flushall()
        r.send_rules('track', self.fixed_track_rules)
        r.send_rules('follow', self.fixed_follow_rules)

        self.follow_tw_rules = [
            {'tag': 'random', 'value': '34534509889'},
            {'tag': 'marketing', 'value': '345345234'}
            ]
        self.track_tw_rules = [
            {'tag': 'short link', 'value': 'dump to'},
            {'tag': 'ref_keywords', 'value': 'something random'}
            ]

    def test_track(self):
        """ Test that track rules parse correctly per the Twitter API. """
        tw_rules = get_redis_twitter_rules('track', '0', 'localhost')
        self.assertEqual(tw_rules, self.track_tw_rules)

    def test_follow(self):
        """ Test that the follow rules parse correctly per the Twitter API. """
        tw_rules = get_redis_twitter_rules('follow', '0', 'localhost')
        self.assertEqual(tw_rules, self.follow_tw_rules)


class TestKafkaProducer(TestCase):
    def test_producer(self):
        """ Test that we can connect to Kafka and create a producer. """
        kafka_producer = create_kafka_producer('localhost:9092', 'traptor123')
        self.assertIsInstance(kafka_producer, SimpleProducer)


class TestBirdyStream(TestCase):
    """ Test that we can create a birdy stream. """
    def setUp(self):
        self.track_tw_rules = 'dump to,something random'
        self.follow_tw_rules = '345345234,34534509889'

    def test_birdy_track(self):
        """ Test that we can create a birdy follow stream. """
        birdy_stream = create_birdy_stream(self.track_tw_rules, 'track', '0')
        self.assertIsInstance(birdy_stream, MyClient)

    def test_birdy_follow(self):
        """ Test that we can create a birdy track stream. """
        birdy_stream = create_birdy_stream(self.follow_tw_rules, 'follow', '0')
        self.assertIsInstance(birdy_stream, MyClient)
