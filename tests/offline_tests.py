# from nose.tools import assert_equal
from unittest import TestCase
from mock import MagicMock

from scripts.rule_extract import CooperRules, RulesToRedis
from sample_rules import FOLLOW_RULES, TRACK_RULES


class TestRuleExtract(TestCase):

    def setUp(self):

        self.follow_rules = FOLLOW_RULES
        self.track_rules = TRACK_RULES

        self.fixed_follow_rules = [
                         {'tag': 'marketing', 'value': '345345234'},
                         {'tag': 'random', 'value': '34534509889'}
                         ]
        self.fixed_track_rules = [
                         {'tag': 'short link', 'value': 'dump to'},
                         {'tag': 'ref_keywords', 'value': 'something random'}
                         ]

    def test_fix_follow(self):
        """ Testing that the "follow" rules get normalized to Twitter format.
        """
        fixed_rules = CooperRules._fix_follow(self.follow_rules)
        self.assertEqual(fixed_rules, self.fixed_follow_rules)

    def test_fix_track(self):
        """ Testing that the "track" rules get normalized to Twitter format.
        """
        fixed_rules = CooperRules._fix_track(self.track_rules)
        self.assertEqual(fixed_rules, self.fixed_track_rules)

    def test_send_to_redis(self):
        """ Testing the logic that sends the rules to Redis. """
        # rc1 = RulesToRedis('track', self.fixed_track_rules)
        raise NotImplementedError