# from nose.tools import assert_equal
from unittest import TestCase
from mock import MagicMock
from scripts.rule_extract import CooperRules, RulesToRedis


class TestRuleExtract(TestCase):

    def setUp(self):
        follow_rule_0 = {
            'tag': 'marketing',
            'value': 'from:345345234 OR someguy'
        }

        follow_rule_1 = {
            'tag': 'sales',
            'value': 'someguy'
        }

        follow_rule_2 = {
            'tag': 'random',
            'value': 'from:34534509889 OR someguy'
        }

        track_rule_0 = {
            'tag': 'short link',
            'value': 'url_contains: dump.to'
        }

        track_rule_1 = {
            'tag': 'ref_keywords',
            'value': 'something random'
        }

        self.follow_rules = [follow_rule_0, follow_rule_1, follow_rule_2]
        self.track_rules = [track_rule_0, track_rule_1]

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

