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

from settings import cooper_settings, redis_settings
import requests
import redis
import logging

logging.basicConfig(level=logging.INFO)


def get_rule_list_from_cooper(url):
    """Get the list of rules from Cooper."""
    payload = {'status': 'active', 'public_format': True}
    # TODO: add code to handle errors when querying Cooper
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        # Parse the response into a dictionary, and return it
        data = response.json()
        follow_ids = {}

        for i in range(0, len(data['data'])):
            if data['data'][i]['type'] == 'follow':
                follow_ids[data['data'][i]['tag']] = data['data'][i]['value']

        return follow_ids
    else:
        # Throw an error and log it
        pass


def get_rule_list_from_redis(redis_conn):
    """Get the list of rules from Redis."""
    rules = redis_conn.keys("traptor-follow:0:*")
    return rules


def add_rule_to_redis(rule_to_add):
    """Add a rule in Redis."""
    # TODO: add the given rule in Redis; add message to Redis
    pass


def update_rule_in_redis(rule_to_update):
    """Update a rule in Redis."""
    # TODO: update the given rule in Redis; add message to Redis
    pass


def delete_rule_in_redis(rule_to_delete):
    """Delete a rule from Redis."""
    # TODO: delete the given rule in Redis; add message to Redis
    pass


def add_message_to_redis(message, message_type):
    """Add a message to Kafka for a Traptor."""
    # TODO: write the code to write to a Kafka topic
    pass


def compare_and_manage_rules(cooper_url, redis_conn):
    """
    Compare two lists of rules and act per the rules below.

    If an existing rule changed: update the rule in Redis
    If an existing rule was deleted: remove the rule in Redis
    If a new rule has been added: add the rule in Redis

    All scenarios other than no changes result in adding a restart message in
    Kafka for the appropriate Traptor.
    """
    # TODO: get the list of rules from Redis and do the comparison
    cooper_rules = get_rule_list_from_cooper(cooper_url)
    redis_rules = get_rule_list_from_redis(redis_conn)
    print cooper_rules, redis_rules


def main():
    """Run the rule manager."""
    cooper_url = cooper_settings['RULES_URL']

    redis_conn = redis.StrictRedis(host=redis_settings['HOST'],
                                   port=redis_settings['PORT'],
                                   db=redis_settings['DB'])

    compare_and_manage_rules(cooper_url, redis_conn)

if __name__ == '__main__':
    main()
