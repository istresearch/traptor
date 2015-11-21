from nose.tools import *
from scripts.rule_extract import CooperRules, send_to_redis

follow_rule_0 = {
    'tag': 'marketing',
    'value': 'from:345345234 OR someguy'
}

track_rule_0 = {
    'tag': 'short link',
    'value': 'url_contains: dump.to'
}

follow_rules = [follow_rule_0]
track_rules = [track_rule_0]


def test_fix_follow():
    """ Testing that the "follow" rules get normalized to Twitter format. """
    c = CooperRules()
    fixed_rules = c._fix_follow(follow_rules)
    assert_equal(fixed_rules, [{'tag': 'marketing', 'value': '345345234'}])


def test_fix_track():
    """ Testing that the "track" rules get normalized to Twitter format. """
    c = CooperRules()
    fixed_rules = c._fix_track(track_rules)
    assert_equal(fixed_rules, [{'tag': 'short link', 'value': 'dump to'}])
