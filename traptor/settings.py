'''
Settings for traptor
====================
'''

import os

# Traptor defaults
TRAPTOR_TYPE = 'track'                          # Type of traptor; options: follow, track, locations
TRAPTOR_ID = 0                                  # Unique id of this traptor
RULE_CHECK_INTERVAL = 60                        # Number of second to check for Rules in Redis (while Traptor sits idle)


# Sentry URL
USE_SENTRY = 'False'                            # Use Sentry for alerting; options: True, False (in single quotes)
SENTRY_URL = ""                              # URL for Sentry if using it for application alerting

# Logging settings
LOG_MAX_BYTES = '10MB'
LOG_BACKUPS = 5
LOG_STDOUT = 'False'
LOG_JSON = 'True'
LOG_LEVEL = 'INFO'                              # Default log level
LOG_DIR = '/tmp'                                # Directory to store traptor logs
LOG_NAME = 'traptor'
LOG_FILE = 'traptor.log'

# Kafka
KAFKA_ENABLED = 'True'
KAFKA_HOSTS = "localhost:9092"                  # Kafka host(s)
KAFKA_TOPIC = "traptor"                         # Kafka topic to write all twitter data

# Redis
REDIS_HOST = "localhost"                        # Redis host
REDIS_PORT = 6379                               # Redis port
REDIS_DB = 0                                    # Redis database where traptor will look for its rules
REDIS_PUBSUB_CHANNEL = "traptor-notify"         # Redis pubsub channel for restart notifications


# Twitter API Keys
# Fill this out in localsettings.py!
APIKEYS = {
    'CONSUMER_KEY': "",
    'CONSUMER_SECRET': "",
    'ACCESS_TOKEN': "",
    'ACCESS_TOKEN_SECRET': ""
}

DW_ENABLED = bool(os.getenv('DW_ENABLED', 'True') == 'True')
DW_CONFIG = {
    'name': os.getenv('DW_NAME', 'traptor'),
    'options': {
        'statsd_host': os.getenv('DW_STATSD_HOST', 'statsd'),
        'statsd_port': os.getenv('DW_STATSD_PORT', 8125),
        'local': bool(os.getenv('DW_LOCAL', 'True') == 'True')
    },
    'metrics': {
        'counters': [
            ('Setting up birdy connection', 'setting_up_birdy_connection'),
            ('Setting up kafka connection', 'setting_up_kafka_connection'),
            ('Caught Kafka Unavailable Error', 'kafka_unavailable_error'),
            ('Skipping kafka connection setup', 'skipping_kafka_connection_setup'),
            ('Tweet sent to kafka', 'tweet_sent_to_kafka'),
            ('Caught Kafka exception when sending a tweet to Kafka', 'kafka_exception_when_sending_tweet'),
            ('Creating birdy follow stream', 'creating_birdy_follow_stream'),
            ('Creating birdy track stream', 'creating_birdy_track_stream'),
            ('Creating birdy locations stream', 'creating_birdy_locations_stream'),
            ('Caught Twitter Api Error creating follow stream', 'twitter_api_error_creating_follow_stream'),
            ('Caught Twitter Api Error', 'twitter_api_error'),
            ('Caught error creating birdy stream for Traptor type that does not exist', 'error_creating_birdy_stream_for_invalid_traptor_type'),
            ('Making the rule counters', 'making_rule_counters'),
            ('Caught exception while incrementing a rule counter', 'exception_incrementing_rule_counter'),
            ('Caught exception while deactivating a rule counter', 'exception_deactivating_rule_counter'),
            ('Caught exception while stopping and deleting a rule counter', 'exception_stopping_and_deleting_rule'),
            ('Rule counters deleted successfully', 'rule_counters_deleted_successfully'),
            ('Caught exception while incrementing a limit counter', 'exception_incrementing_limit_counte'),
            ('Caught exception while performing rule matching for track', 'exception_performing_track_rule_matching'),
            ('Unable to dump the tweet dict to json', 'unable_to_dump_tweet_dict_to_json'),
            ('Caught exception while performing rule matching for follow', 'exception_performing_follow_rule_matching'),
            ('No rule matched for tweet', 'no_rule_matched_for_tweet'),
            ('Unsupported traptor_type', 'unsupported_traptor_type'),
            ('Getting rules from Redis', 'getting_rules_from_redis'),
            ('Caught exception while connecting to Redis', 'exception_connecting_to_redis'),
            ('Twitter message is not a tweet', 'twitter_message_is_not_tweet'),
            ('Subscribing to the Traptor notification PubSub', 'subscribing_to_traptor_notification_pubsub'),
            ('Starting the heartbeat', 'starting_heartbeat'),
            ('Caught exception while adding the heartbeat message to Redis', 'exception_adding_heartbeat_message'),
            ('Attempting to send tweet to kafka', 'attempting_to_send_tweet_to_kafka'),
            ('Starting tweet processing', 'starting_tweet_processing'),
            ('Caught exception while json loading the Twitter message', 'exception_json_loading_the_twitter_message'),
            ('Caught exception adding Twitter message to Kafka', 'exception_adding_twitter_message_to_kafka'),
            ('Restart flag is true; restarting myself', 'restart_flag_true_restarting'),
            ('Waiting for rules', 'waiting_for_rules'),
            ('Starting Traptor', 'starting_traptor'),
            ('Caught exception when starting Traptor', 'exception_starting_traptor'),
        ]
    }
}


# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
