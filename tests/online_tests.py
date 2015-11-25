# from nose.tools import assert_equal
import json
from unittest import TestCase
from mock import MagicMock
from kafka import SimpleProducer
from redis import StrictRedis, ConnectionError

from scripts.rule_extract import CooperRules, RulesToRedis
from sample_rules import FOLLOW_RULES, TRACK_RULES
from traptor.traptor import (MyClient, get_redis_twitter_rules,
                             create_kafka_producer,
                             create_birdy_stream, clean_tweet_data, run,
                             tweet_time_to_iso
                             )

from traptor.settings import (KAFKA_HOSTS, KAFKA_TOPIC, APIKEYS, TRAPTOR_ID,
                      TRAPTOR_TYPE, REDIS_HOST)
from traptor.birdy.twitter import StreamResponse


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


    def test_follow(self):
        """ Test that FOLLOW rules parse correctly per the Twitter API. """
        tw_rules = get_redis_twitter_rules('follow', '0', 'localhost')
        self.assertEqual(sorted(tw_rules), sorted(self.follow_tw_rules))

    def test_track(self):
        """ Test that TRACK rules parse correctly per the Twitter API. """
        tw_rules = get_redis_twitter_rules('track', '0', 'localhost')
        self.assertEqual(sorted(tw_rules), sorted(self.track_tw_rules))


class TestKafkaProducer(TestCase):
    def test_producer(self):
        """ Test that we can connect to Kafka and create a producer. """
        kafka_producer = create_kafka_producer('localhost:9092', 'traptor123')
        self.assertIsInstance(kafka_producer, SimpleProducer)


class TestBirdyStream(TestCase):
    """ Test that we can create a birdy stream. 
        To do:  mock the twitter connection so that we don't get rate limited.
    """
    def setUp(self):
        self.track_tw_rules = 'dump to,something random'
        self.follow_tw_rules = '345345234,34534509889'

    def test_birdy_follow(self):
        """ Test that we can create a birdy FOLLOW stream. """
        birdy_stream = create_birdy_stream(self.follow_tw_rules, APIKEYS,
                                           'follow', '0')
        self.assertIsInstance(birdy_stream, StreamResponse)

    def test_birdy_track(self):
        """ Test that we can create a birdy TRACK stream. """
        birdy_stream = create_birdy_stream(self.track_tw_rules, APIKEYS,
                                           'track', '1')
        self.assertIsInstance(birdy_stream, StreamResponse)

class TestCleanTweetData(TestCase):
    def setUp(self):
        self.twitter_time = "Wed Nov 25 19:36:51 +0000 2015"
        self.iso_time = "2015-11-25T19:36:51+00:00"

        with open('tests/sample_raw_tweets.json') as f:
            self.raw_tweets = [json.loads(line) for line in f]

        with open('tests/sample_cleaned_tweets.json') as f:
            self.cleaned_tweets = [json.loads(line) for line in f]

    def test_tweet_time_to_iso(self):
        """ Test that the default twitter time format is converted to ISO. """
        self.assertEqual(tweet_time_to_iso(self.twitter_time), self.iso_time)

    def test_clean_tweet_data(self):
        """ That that the raw tweet data is cleaned according to the expected
            format.
        """
        self.assertItemsEqual(clean_tweet_data(self.raw_tweets[0]),
                              self.cleaned_tweets[0])

    def test_add_rule_tags(self):
        raise NotImplementedError
