"""Test Rule Manager."""
# To run with autotest, run: py.test --looponfail

import pytest
from rule_manager.settings import (redis_settings, cooper_settings,
                                   sentry_settings, rule_refresh_settings,
                                   traptor_pubsub_settings)
import redis
from rule_manager.rule_manager import RuleManager
from rule_manager.rule_manager import run_rule_manager
from rule_manager.rule_extract import RulesToRedis
from scutils.log_factory import LogObject
import json


@pytest.fixture()
def cooper_url():
    """Create a connection to the Redis PubSub."""
    cooper_url = cooper_settings['RULES_URL']
    return cooper_url


@pytest.fixture()
def redis_conn():
    """Create a connection to Redis."""
    redis_conn = redis.StrictRedis(host=redis_settings['HOST'],
                                   port=redis_settings['PORT'],
                                   db=5)
    return redis_conn


@pytest.fixture()
def redis_pubsub():
    """Create a connection to the Redis pubsub."""
    redis_pubsub = redis.StrictRedis(host=redis_settings['HOST'],
                                     port=redis_settings['PORT'],
                                     db=5)
    return redis_pubsub


@pytest.fixture()
def redis_rules(request):
    """Load up some sample traptor rules into Redis."""
    with open('tests/data/track_rules.json') as f:
        track_rules = [json.loads(line) for line in f]
    with open('tests/data/follow_rules.json') as f:
        follow_rules = [json.loads(line) for line in f]
    with open('tests/data/locations_rules.json') as f:
        locations_rules = [json.loads(line) for line in f]

    conn = redis.StrictRedis(host=redis_settings['HOST'],
                             port=redis_settings['PORT'],
                             db=5)
    conn.flushdb()

    rc = RulesToRedis(conn)
    rc.send_rules(traptor_type='track', rules=track_rules)
    rc.send_rules(traptor_type='follow', rules=follow_rules)
    rc.send_rules(traptor_type='locations', rules=locations_rules)

    def cleanup():
        conn.flushdb()

    request.addfinalizer(cleanup)

    return conn


@pytest.fixture()
def tn_channel():
    """Get the Traptor pubsub settings."""
    tn_channel = traptor_pubsub_settings['CHANNEL_NAME']
    return tn_channel


@pytest.fixture()
def log_level():
    """Provide the log level for the application."""
    return 'INFO'


@pytest.fixture()
def sentry_client():
    """Create a connection to Sentry."""
    if sentry_settings['USE_SENTRY'] == 'False':
        sentry_client = 'http://www.example.com'
    else:
        sentry_client = sentry_settings['SENTRY_URL']
    return sentry_client


@pytest.fixture()
def rule_refresh_rate():
    """Determine the rule refresh rate."""
    refresh_rate = rule_refresh_settings['RULE_REFRESH_TIME']
    return refresh_rate


@pytest.fixture()
def rule_manager(request,
                 cooper_url,
                 redis_conn,
                 tn_channel,
                 log_level,
                 sentry_client):
    """Create an instance of the rule manager."""
    rule_manager_instance = RuleManager(cooper_url=cooper_url,
                                        redis_conn=redis_conn,
                                        traptor_notify_channel=tn_channel,
                                        log_level=log_level,
                                        sentry_client=sentry_client)

    return rule_manager_instance


def test_run_rule_manager(cooper_url,
                          redis_conn,
                          tn_channel,
                          log_level,
                          sentry_client):
        """Test that we can run rule manager."""
        run_rule_manager(cooper_url,
                         redis_conn,
                         tn_channel,
                         log_level,
                         sentry_client)


class TestRuleManager(object):
    """Test Rule Manager."""

    def test_setup(self, rule_manager):
        """Test the setup function."""
        rule_manager._setup()
        assert isinstance(rule_manager.logger, LogObject)

    def test_get_rules_from_cooper(self, rule_manager, cooper_url):
        """Test obtaining rules from the Cooper api."""
        rule_manager._setup()
        rule_manager.cooper_url = cooper_url
        c_follow_rules, c_track_rules, c_location_rules = \
            rule_manager._get_rules_from_cooper()
        assert len(c_follow_rules) == 1
        assert len(c_track_rules) == 7
        assert len(c_location_rules) == 1

    def test_get_traptor_rules_from_redis(self,
                                          rule_manager,
                                          redis_conn,
                                          redis_rules):
        """Test obtaining the full list of rules from Redis."""
        rule_manager._setup()
        follow_rules, tracker_rules, location_rules = \
            rule_manager._get_traptor_rules_from_redis()
        assert len(follow_rules) == 1
        assert len(tracker_rules) == 1
        assert len(location_rules) == 1

    def test_get_traptor_rules_with_keys_from_redis(self,
                                                    rule_manager,
                                                    redis_conn,
                                                    redis_rules):
        """Test getting the complete list of Traptor rules from Redis."""
        rule_manager._setup()
        all_traptor_rules = \
            rule_manager._get_traptor_rules_with_keys_from_redis()
        assert len(all_traptor_rules) == 3

    def test_determine_rules_to_add_and_delete(self,
                                               cooper_url,
                                               redis_rules):
        """Test the rule comparison."""
        # Populate the Cooper database with the rules from Redis. Mark them as inactive.
        # Add two new rules for each rule type
        # Run the method and check that the counts are correct
        pass
