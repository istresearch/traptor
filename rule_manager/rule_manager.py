#!/usr/bin/env python

"""
Manage the Traptor rules in Redis.

Rule manager maintains an up-to-date list of Traptor rules in Redis,
and creates a message in Redis which can be consumed by a running Traptor.

Rule manager will:

1. Detect a change in the rules stored in Cooper.
    - Pull the latest rules from the Cooper API
    - Compare against the existing rules
2. Update the rules in Redis as necessary
    - Create
    - Update
    - Delete
3. Add a message to Redis for the appropriate Traptor.

"""

from settings import (redis_settings, cooper_settings, sentry_settings,
                      rule_refresh_settings, traptor_pubsub_settings)
import requests
import redis
import unicodedata
from collections import Counter
from scutils.log_factory import LogFactory
from raven import Client
import time
import click
import sys

import warnings
warnings.filterwarnings('error', category=UnicodeWarning)


class RuleManager(object):
    """Rule manager base class."""

    def __init__(self,
                 cooper_url,
                 redis_conn,
                 traptor_notify_channel,
                 log_level,
                 sentry_client):
        """
        Rulemanager base class.

        :param str cooper_url: cooper url to use.
        :param str redis_conn: redis connection to use.
        :param str traptor_notify_channel: name of the Traptor notify channel.
        :param str log_level: logging level.
        :param str sentry_client: Sentry url

        """
        self.cooper_url = cooper_url
        self.redis_conn = redis_conn
        self.traptor_notify_channel = traptor_notify_channel
        self.log_level = log_level
        self.sentry_client = sentry_client

    def __repr__(self):
        """Represent yourself."""
        return 'RuleManager({}, {}, {}, {}, {})'.format(
            self.cooper_url,
            self.redis_conn,
            self.traptor_notify_channel,
            self.log_level,
            self.sentry_client
        )

    def _setup(self):
        """Load everything up."""
        # Set up logging
        self.logger = LogFactory.get_instance(name='traptor-rule-manager',
                                              level=self.log_level)
        self.logger.info('Finished set up. Continuing on.')

    def _normalize_unicode_funk(self, item):
        """Normalize the unicode funk."""
        if isinstance(item, unicode):
            item = unicodedata.normalize('NFKD', item).encode('utf-8')
        return (item)

    def _get_rules_from_cooper(self):
        """Get the list of rules from Cooper."""
        self.logger.info('Getting list of rules from Cooper')
        payload = {'status': 'active', 'public_format': True}
        response = requests.post(self.cooper_url, json=payload)
        if response.status_code == 200:
            data = response.json()
            follow_ids = []
            track_ids = []
            location_ids = []

            for i in data['data']:
                tag = i['tag']
                value = i['value']

                if i['type'] == 'follow':
                    follow_ids.append([tag, value])
                elif i['type'] == 'track':
                    track_ids.append([tag, value])
                elif i['type'] == 'locations':
                    location_ids.append([tag, value])

            return (follow_ids, track_ids, location_ids)
        else:
            # TODO: add code to handle errors when querying Cooper
            pass

    # Get the Traptor rules from Redis
    def _get_traptor_rules_from_redis(self):
        """Get the list of Traptor rules from Redis."""
        self.logger.info('Getting list of rules from Traptor')
        follow_rules = []
        tracker_rules = []
        location_rules = []
        r_follows = self.redis_conn.keys("traptor-follow:*")
        r_trackers = self.redis_conn.keys("traptor-track*")
        r_locations = self.redis_conn.keys("traptor-locations*")

        # For each key in the list, get the tag and value and add to the list

        for key in r_follows:
            i = self.redis_conn.hgetall(key)
            follow_rules.append([i['tag'], i['value']])

        for key in r_trackers:
            i = self.redis_conn.hgetall(key)
            tracker_rules.append([i['tag'], i['value']])

        for key in r_locations:
            i = self.redis_conn.hgetall(key)
            location_rules.append([i['tag'], i['value']])

        return (follow_rules, tracker_rules, location_rules)

    def _get_traptor_rules_with_keys_from_redis(self):
        """Get the complete list of Traptor rules from Redis."""
        self.logger.info('Getting the complete list of rules from Redis')
        all_redis_rules = []

        rule_list = self.redis_conn.keys("traptor*")

        for key in rule_list:
            i = self.redis_conn.hgetall(key)
            all_redis_rules.append([key, i['tag'], i['value']])

        return all_redis_rules

    def _determine_rules_to_add_and_delete(self, rule_type, source_rules, traptor_rules):
        """Determine which rules need to be added for Traptor and which deleted."""
        self.logger.info('Determining which Traptor {} rules to add and delete'.format(rule_type))
        rules_to_add = []
        rules_to_delete = []

        # In c and not in r -> new
        for rule in source_rules:
            try:
                if rule not in traptor_rules:
                    rules_to_add.append(rule)
            except UnicodeWarning:
                self.logger.error('UnicodeWarning during new rule check for {} rule {}'.format(rule_type, rule))

        # In r and not in c -> deleted
        for rule in traptor_rules:
            try:
                if rule not in source_rules:
                    rules_to_delete.append(rule)
            except UnicodeWarning:
                self.logger.error('UnicodeWarning during delete rule check for {} rule {}'.format(rule_type, rule))

        return (rules_to_add, rules_to_delete)

    def _get_traptor_number(self, traptor_rule):
        """Get the Traptor number from a rule."""
        t = traptor_rule[0].split(':')

        traptor_type = t[0].split('-')[1]
        traptor_id = t[1]

        return traptor_type, traptor_id

    def _delete_rule_in_redis(self, rule_to_delete):
        """Delete a rule from Redis."""
        if self.redis_conn.delete(rule_to_delete) == 1:
            self.logger.info('Deleted a Traptor rule from Redis: {}'.format(rule_to_delete))
            return 1
        else:
            self.logger.error('Unable to delete Traptor rule from Redis: {}'.format(rule_to_delete))
            return 0

    def _add_rule_to_redis(self, hash_to_add, rule_to_add):
        """Add a rule in Redis."""
        try:
            self.redis_conn.hmset(hash_to_add, rule_to_add)
            self.logger.info('Added a Traptor rule to Redis: {} - {}'.format(hash_to_add, rule_to_add))
        except:
            self.logger.error('Unable to add a new Traptor rule to Redis: {} - {}'.format(hash_to_add, rule_to_add))

    def _add_message_to_redis(self, channel, message):
        """Add a PubSub message to Redis."""
        try:
            self.redis_conn.publish(channel, message)
            self.logger.info('Added a Traptor message to Redis. Channel: {}; Message: {}'.format(channel, message))
        except:
            self.logger.error('Unable to add a new Traptor message to Redis: Channel: {}; Message: {}'.format(channel, message))

    def _add_error_to_sentry(self, message):
        """Send a critical error message to Sentry."""
        # TODO: connect with our logging infrastructure
        try:
            self.sentry_client.captureMessage(message)
            self.logger.info('Message sent to Sentry: {}'.format(message))
        except:
            self.logger.error('Unable to communicate with Sentry to send message: '.format(message))

    def _get_traptor_hash_id(self, hash_lookup):
        """Get the hash key of a given Traptor."""
        max_hash_id = sorted(self.redis_conn.keys(hash_lookup), reverse=True)[0]
        return int(max_hash_id.split(':')[2])

    def run(self):
        """Run the Rule Manager instance."""
        # Setup connections and logging
        self._setup()

        self.logger.info('Processing rules to delete.')

        # Get all of the data to compare
        c_follow_rules, c_track_rules, c_location_rules = self._get_rules_from_cooper()
        r_follow_rules, r_track_rules, r_location_rules = self._get_traptor_rules_from_redis()

        # Determine which source rules need to be added and which deleted
        follow_rules_to_add, follow_rules_to_delete = self._determine_rules_to_add_and_delete('follow', c_follow_rules, r_follow_rules)
        track_rules_to_add, track_rules_to_delete = self._determine_rules_to_add_and_delete('track', c_track_rules, r_track_rules)
        location_rules_to_add, location_rules_to_delete = self._determine_rules_to_add_and_delete('location', c_location_rules, r_location_rules)

        # Get all of the traptor rules with their keys so we can look them up
        all_traptor_rules = self._get_traptor_rules_with_keys_from_redis()

        # Create the list of rules to delete and the traptors to notify
        traptor_rules_to_delete = []
        traptors_to_notify_of_delete = []

        # Follow Rules
        for r in follow_rules_to_delete:
            traptor_rules_to_delete.append([item[0] for item in all_traptor_rules if item[1] == r[0] and item[2] == r[1]])

        # Track Rules
        for r in track_rules_to_delete:
            traptor_rules_to_delete.append([item[0] for item in all_traptor_rules if item[1] == r[0] and item[2] == r[1]])

        # Location Rules
        for r in location_rules_to_delete:
            traptor_rules_to_delete.append([item[0] for item in all_traptor_rules if item[1] == r[0] and item[2] == r[1]])

        # Create the list of Traptors to notify
        # ((type, id), rule_count)
        for r in Counter(map(self._get_traptor_number, traptor_rules_to_delete)).items():
            t_type = r[0][0]
            t_id = r[0][1]
            t_notify = t_type + ":" + t_id
            traptors_to_notify_of_delete.append(t_notify)

        # Delete the Traptor rules in Redis
        for rule_key in traptor_rules_to_delete:
            self._delete_rule_in_redis(rule_key[0])

        """
        Now that we've deleted the rules we need to delete,
        re-run the comparison for the adds
        """

        self.logger.info('Processing rules to add.')

        c_follow_rules, c_track_rules, c_location_rules = \
            self._get_rules_from_cooper()
        r_follow_rules, r_track_rules, r_location_rules = \
            self._get_traptor_rules_from_redis()

        # Determine which source rules need to be added and which deleted
        follow_rules_to_add, follow_rules_to_delete = \
            self._determine_rules_to_add_and_delete('follow',
                                                    c_follow_rules,
                                                    r_follow_rules)
        track_rules_to_add, track_rules_to_delete = \
            self._determine_rules_to_add_and_delete('track',
                                                    c_track_rules,
                                                    r_track_rules)
        location_rules_to_add, location_rules_to_delete = \
            self._determine_rules_to_add_and_delete('location',
                                                    c_location_rules,
                                                    r_location_rules)

        # Handle the source rules we need to add
        available_traptors = []
        twitter_thresholds = {'follow': 5000, 'track': 400, 'locations': 25}

        # Get all of the traptor rules with their keys so we can look them up
        all_traptor_rules = self._get_traptor_rules_with_keys_from_redis()

        # Get the current Traptor load capacity
        # ((type, id), rule_count)
        current_load = Counter(map(self._get_traptor_number, all_traptor_rules))

        # Create a list of available Traptors
        for r in current_load.items():
            t_type = r[0][0]
            t_id = r[0][1]
            t_load = r[1]
            t_available = twitter_thresholds[t_type] - t_load

            t_notify = t_type + ":" + t_id + ":" + str(t_available)

            if t_type == 'follow' and t_load < twitter_thresholds['follow']:
                available_traptors.append(t_notify)
            elif t_type == 'track' and t_load < twitter_thresholds['track']:
                available_traptors.append(t_notify)
            elif t_type == 'locations' and t_load < twitter_thresholds['locations']:
                available_traptors.append(t_notify)

        # Sort the traptors in order of capacity
        available_traptors = sorted(available_traptors, key=lambda traptor: traptor[2])

        """
        Create Redis rules for the appropriate Traptors, add to Redis, and
        add the Traptors to the notify list

        TODO: handle instances where we don't have enough Traptors for the
              particular rule type

        For each type of traptor:
            1. Compare the number of rules we need to add to the current
               Traptor capacity
            2. If the Traptor with the highest capacity can handle the load
                -> assign the rules to it (KISS)
            3. If no Traptors can handle the rules
                -> send a message to Sentry

        Later: if no single Traptor can handle all the rules, assign the
               highest amount to the Traptor with the most capacity,
               then assign the remaining load to the next Traptor, and so on.
        """

        traptors_to_notify_of_add = []

        new_follow_rule_count = len(follow_rules_to_add)
        new_track_rule_count = len(track_rules_to_add)
        new_location_rule_count = len(location_rules_to_add)

        follow_traptor = None
        track_traptor = None
        locations_traptor = None

        follow_traptor = next(t for t in available_traptors if t.split(':')[0] == 'follow' and t.split(':')[2] > new_follow_rule_count)
        track_traptor = next(t for t in available_traptors if t.split(':')[0] == 'track' and t.split(':')[2] > new_track_rule_count)
        locations_traptor = next(t for t in available_traptors if t.split(':')[0] == 'locations' and t.split(':')[2] > new_location_rule_count)

        for rule in follow_rules_to_add:
            traptor_type, traptor_id, capacity = follow_traptor.split(':')

            # Get the new hash id for the traptor
            lookup = 'traptor-{}:{}:*'.format(traptor_type, traptor_id)
            new_hash_id = self._get_traptor_hash_id(lookup) + 1

            # Create a new hash for the traptor
            new_traptor_hash = 'traptor-{}:{}:{}'.format(traptor_type,
                                                         traptor_id,
                                                         new_hash_id)
            new_rule = {'tag': rule[0], 'value': rule[1]}

            self._add_rule_to_redis(new_traptor_hash, new_rule)

            # Add the Traptor to the notify list
            traptors_to_notify_of_add.append('{}:{}'.format(traptor_type,
                                                            traptor_id))

        for rule in track_rules_to_add:
            traptor_type, traptor_id, capacity = track_traptor.split(':')

            # Get the new hash id for the traptor
            lookup = 'traptor-{}:{}:*'.format(traptor_type, traptor_id)
            new_hash_id = self._get_traptor_hash_id(lookup) + 1

            # Create a new hash for the traptor
            new_traptor_hash = 'traptor-{}:{}:{}'.format(traptor_type,
                                                         traptor_id,
                                                         new_hash_id)
            new_rule = {'tag': rule[0], 'value': rule[1]}

            self._add_rule_to_redis(new_traptor_hash, new_rule)

            # Add the Traptor to the notify list
            traptors_to_notify_of_add.append('{}:{}'.format(traptor_type,
                                                            traptor_id))

        for rule in location_rules_to_add:
            traptor_type, traptor_id, capacity = locations_traptor.split(':')

            # Get the new hash id for the traptor
            lookup = 'traptor-{}:{}:*'.format(traptor_type, traptor_id)
            new_hash_id = self._get_traptor_hash_id(lookup) + 1

            # Create a new hash for the traptor
            new_traptor_hash = 'traptor-{}:{}:{}'.format(traptor_type,
                                                         traptor_id,
                                                         new_hash_id)
            new_rule = {'tag': rule[0], 'value': rule[1]}

            self._add_rule_to_redis(new_traptor_hash, new_rule)

            # Add the Traptor to the notify list
            traptors_to_notify_of_add.append('{}:{}'.format(traptor_type,
                                                            traptor_id))

        """
        Add a restart message to the Redis PubSub for each Traptor
        in the notify list
        """
        traptors_to_notify = set(traptors_to_notify_of_delete +
                                 traptors_to_notify_of_add)

        if len(traptors_to_notify) > 0:
            for traptor in traptors_to_notify:
                self._add_message_to_redis(self.traptor_notify_channel,
                                           traptor)

        self.logger.info("Finished running!")


def run_rule_manager(log_level):
    """Run the rule manager."""
    cooper_url = cooper_settings['RULES_URL']

    # Redis connection
    redis_conn = redis.StrictRedis(host=redis_settings['HOST'],
                                   port=redis_settings['PORT'],
                                   db=redis_settings['DB'])

    # Redis PubSub channel to notify the traptors
    traptor_notify_channel = traptor_pubsub_settings['CHANNEL_NAME']

    # Logging
    log_level = log_level

    # Set up a connection to Sentry if the user wants to use it
    if sentry_settings['USE_SENTRY'] == 'True':
        sentry_client = Client(sentry_settings['SENTRY_URL'])
    else:
        sentry_client = None

    # Create a new rule manager
    rm = RuleManager(cooper_url=cooper_url,
                     redis_conn=redis_conn,
                     traptor_notify_channel=traptor_notify_channel,
                     log_level=log_level,
                     sentry_client=sentry_client)

    # Run it
    rm.run()


@click.command()
@click.option('--info', is_flag=True)
@click.option('--debug', is_flag=True)
def main(info, debug):
    """Command line interface to run a Rule Manager instance.

        Can pass it flags for debug levels
    """
    if debug:
        log_level = 'DEBUG'
    elif info:
        log_level = 'INFO'
    else:
        log_level = 'CRITICAL'

    """Run the rule manager."""
    # Rule manager refresh rate
    refresh_rate = rule_refresh_settings['RULE_REFRESH_TIME']

    while True:
        run_rule_manager(log_level)
        time.sleep(refresh_rate)

if __name__ == '__main__':
    sys.exit(main())
