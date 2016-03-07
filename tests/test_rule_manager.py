"""Test Rule Manager."""
# IMPORTANT: Ensure that Cooper is pointing to the test database before running these.
# To run with autotest and print all output to console run: py.test -s --looponfail

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
import pymysql


@pytest.fixture()
def cooper_url():
    """Provide the Cooper URL."""
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
        assert len(c_track_rules) == 1
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
                                               rule_manager,
                                               cooper_url,
                                               redis_rules):
        """Test the rule comparison."""
        # Connect to the Cooper database
        connection = pymysql.connect(host=cooper_settings['TEST_DB_HOST'],
                                     user=cooper_settings['TEST_DB_USER'],
                                     passwd=cooper_settings['TEST_DB_PASSWORD'])

        with connection.cursor() as cursor:
            # Drop the current test database
            cursor.execute('drop database if exists {}'.format(cooper_settings['TEST_DB']))
            # Create the database
            cursor.execute('create database {}'.format(cooper_settings['TEST_DB']))
            cursor.execute('use {}'.format(cooper_settings['TEST_DB']))
            # Create the rules table
            cursor.execute("""CREATE TABLE `rules` (
                              `id` int(11) NOT NULL AUTO_INCREMENT,
                              `user_id` int(11) DEFAULT NULL,
                              `rule_set` varchar(40) DEFAULT NULL,
                              `tag` varchar(100) DEFAULT NULL,
                              `type` varchar(100) DEFAULT NULL,
                              `value` varchar(100) CHARACTER SET utf8 DEFAULT NULL,
                              `description` varchar(100) DEFAULT NULL,
                              `status` varchar(100) DEFAULT NULL,
                              `detasking` varchar(100) DEFAULT NULL,
                              `date_added` datetime DEFAULT NULL,
                              `notes` varchar(100) DEFAULT NULL,
                              `appid` varchar(100) DEFAULT NULL,
                              PRIMARY KEY (`id`),
                              KEY `user_id` (`user_id`)
                           ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""")
        connection.commit()

        # Add the rules to use for testing
        with open('tests/data/track_rules.json') as f:
            track_rules = [json.loads(line) for line in f]
        with open('tests/data/follow_rules.json') as f:
            follow_rules = [json.loads(line) for line in f]
        with open('tests/data/locations_rules.json') as f:
            locations_rules = [json.loads(line) for line in f]

        with connection.cursor() as cursor:
            for rule in track_rules:
                sql = "insert into `rules` (`tag`, `value`, `type`, `status`) values (%s, %s, %s, %s)"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'keyword', 'Active'))
            for rule in follow_rules:
                sql = "insert into `rules` (`tag`, `value`, `type`, `status`) values (%s, %s, %s, %s)"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'username', 'Active'))
            for rule in locations_rules:
                sql = "insert into `rules` (`tag`, `value`, `type`, `status`) values (%s, %s, %s, %s)"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'geo', 'Active'))
        connection.commit()

        # Run the method and check that the counts are correct
        rule_manager._setup()
        rule_manager.cooper_url = cooper_url

        c_follow_rules, c_track_rules, c_location_rules = rule_manager._get_rules_from_cooper()
        r_follow_rules, r_track_rules, r_location_rules = rule_manager._get_traptor_rules_from_redis()

        follow_rules_to_add, follow_rules_to_delete = rule_manager._determine_rules_to_add_and_delete('follow', c_follow_rules, r_follow_rules)
        track_rules_to_add, track_rules_to_delete = rule_manager._determine_rules_to_add_and_delete('track', c_track_rules, r_track_rules)
        location_rules_to_add, location_rules_to_delete = rule_manager._determine_rules_to_add_and_delete('location', c_location_rules, r_location_rules)

        # All things should at first be equal
        assert len(follow_rules_to_add) == 0
        assert len(follow_rules_to_delete) == 0
        assert len(track_rules_to_add) == 0
        assert len(track_rules_to_delete) == 0
        assert len(location_rules_to_add) == 0
        assert len(location_rules_to_delete) == 0

        # Add one of each type of rule to the database
        with connection.cursor() as cursor:
            sql = "insert into `rules` (`tag`, `value`, `type`, `status`) values (%s, %s, %s, %s)"
            cursor.execute(sql, ('another_keyword_test', 'kickass', 'keyword', 'Active'))
            sql = "insert into `rules` (`tag`, `value`, `type`, `status`) values (%s, %s, %s, %s)"
            cursor.execute(sql, ('another_username_test', '48892028', 'username', 'Active'))
            sql = "insert into `rules` (`tag`, `value`, `type`, `status`) values (%s, %s, %s, %s)"
            cursor.execute(sql, ('another_geo_test', '-120.75,36.8,-119.75,37.8', 'geo', 'Active'))
        connection.commit()

        # Mark the initial rules inactive
        with connection.cursor() as cursor:
            for rule in track_rules:
                sql = "update `rules` set `status` = 'Inactive' where `tag` = %s and `value` = %s and `type` = %s"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'keyword'))
            for rule in follow_rules:
                sql = "update `rules` set `status` = 'Inactive' where `tag` = %s and `value` = %s and `type` = %s"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'username'))
            for rule in locations_rules:
                sql = "update `rules` set `status` = 'Inactive' where `tag` = %s and `value` = %s and `type` = %s"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'geo'))
        connection.commit()

        # Recreate the rule lists
        c_follow_rules, c_track_rules, c_location_rules = rule_manager._get_rules_from_cooper()
        r_follow_rules, r_track_rules, r_location_rules = rule_manager._get_traptor_rules_from_redis()

        follow_rules_to_add, follow_rules_to_delete = rule_manager._determine_rules_to_add_and_delete('follow', c_follow_rules, r_follow_rules)
        track_rules_to_add, track_rules_to_delete = rule_manager._determine_rules_to_add_and_delete('track', c_track_rules, r_track_rules)
        location_rules_to_add, location_rules_to_delete = rule_manager._determine_rules_to_add_and_delete('location', c_location_rules, r_location_rules)

        # Test that we have one of each rule to add and one of each to delete
        assert len(follow_rules_to_add) == 1
        assert len(follow_rules_to_delete) == 1
        assert len(track_rules_to_add) == 1
        assert len(track_rules_to_delete) == 1
        assert len(location_rules_to_add) == 1
        assert len(location_rules_to_delete) == 1
