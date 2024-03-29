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
KAFKA_TOPIC = "traptor"                         # Kafka topic to write all twitter data

KAFKA_HOSTS = 'localhost:9092'
KAFKA_BROKER_VERSION_FALLBACK = '0.9.0'
KAFKA_API_VERSION_REQUEST = bool(os.getenv('KAFKA_API_VERSION_REQUEST', 'False') == 'True')
KAFKA_PRODUCER_BATCH_LINGER_MS = 1000
KAFKA_PRODUCER_BUFFER_KBYTES = 1048576  # 1024 MB (librdkafka default)
KAFKA_PRODUCER_TOPIC = 'crawl.incoming'
KAFKA_SECURITY_PROTOCOL = None

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

# Rate Limiter
RATE_LIMITING_ENABLED = bool(os.getenv('RATE_LIMITING_ENABLED', 'False') == 'True')
RATE_LIMITING_RATE_SEC = max(1.0, float(os.getenv('RATE_LIMITING_RATE_SEC', 10.0)))
RATE_LIMITING_CAPACITY = max(1, int(os.getenv('RATE_LIMITING_CAPACITY', 10)))
RATE_LIMITING_COST = max(1, int(os.getenv('RATE_LIMITING_COST', 1)))
RATE_LIMITING_REPORTING_INTERVAL_SEC = max(1.0, float(os.getenv('RATE_LIMITING_REPORTING_INTERVAL_SEC', 60.0)))

# Manager API 

API_PORT = os.getenv('API_PORT', 5000)
API_DIR = os.getenv('API_DIR', '.')
API_SPEC = os.getenv('API_SPEC', 'spec.yml')
API_BACKEND = os.getenv('API_BACKEND', 'local')
PROXY_URL = os.getenv('PROXY_URL', 'http://localhost:8080')
PROXY_USER = os.getenv('PROXY_USER', 'test')
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD', 'test')
PROXY_TIMEOUT = int(os.getenv('PROXY_TIMEOUT', 4))
TWITTER_API_URL = 'https://api.twitter.com/1.1'

# Dog Whistle
DWC_SEND_TO_KAFKA_ENRICHED = 'tweet_to_kafka_enriched'
DWC_ERROR_SEND_TO_KAFKA = 'Caught exception adding Twitter message to Kafka'
DWC_RETRY_TWITTER = 'traptor_retry_twitter_api'
DWC_RETRY_REDIS = 'traptor_retry_redis_api'
DWC_RETRY_KAFKA = 'traptor_retry_kakfa_api'
DWG_RULE_COUNT = {'name': 'Traptor Rule Count',
                   'key': 'traptor_rule_count', 'value': 'rule_count'}
# Match Count gauge not in use, yet, was determined to complex to put in now.
DWG_MATCH_COUNT = {'name': 'Traptor Num Matches For ID',
                   'key': 'traptor_num_matches_for_id', 'value': 'match_count'}
DW_ENABLED = bool(os.getenv('DW_ENABLED', 'False') == 'True')
DW_CONFIG = {
    'name': os.getenv('DW_NAME', 'traptor'),
    'metrics': {
        'counters': [
            ('heartbeat_message_sent_success', 'src.heartbeat.success.count'),
            ('Caught exception while adding the heartbeat message to Redis', 'src.heartbeat.failure.count'),
            ('restart_message_received', 'src.restart_message.success.count'),
            ('kafka_error', 'src.kafka.error'),
            ('redis_error', 'src.redis.error'),
            ('traptor_error_occurred', 'src.error.count'),
            ('twitter_error_occurred', 'src.twitter.error.count'),
            ('tweet_process_success', 'src.tweet_process.success'),
            ('tweet_to_kafka_success', 'src.tweet_to_kafka.success'),
            ('tweet_to_kafka_failure', 'src.tweet_to_kafka.failure'),
            ('limit_message_received', 'src.limit.messages.count'),
            ('limit_message_count', 'src.limit.current_limited'),
            ('Stream keep-alive received', 'src.twitter.keepalive'),
            ('Waiting for rules', 'src.rules.waiting'),
            ('Withholding heartbeat, not accepting rule assignments', 'src.rules.not_accepting'),
            ('API credentials are invalid', 'src.auth.invalid'),
            ('Exception caught in redis pub/sub listener, restarting listener', 'src.redis.error'),
            ('Exception caught closing redis pub/sub listener', 'src.redis.error')
        ],
        'gauges': [
            (DWG_RULE_COUNT['name'], DWG_RULE_COUNT['key'], DWG_RULE_COUNT['value']),
            (DWG_MATCH_COUNT['name'], DWG_MATCH_COUNT['key'], DWG_MATCH_COUNT['value']),
            ('redis_ping_status', 'redis_ping_status', 'status.int'),
            ('kafka_ping_status', 'kafka_ping_status', 'status.int'),
            ('twitter_ping_status', 'twitter_ping_status', 'status.int'),
        ]
    },
    'allow_extra_tags': True,
    'options': {
        'statsd_host': os.getenv('DW_STATSD_HOST', 'statsd'),
        'statsd_port': os.getenv('DW_STATSD_PORT', 8125),
        'local': bool(os.getenv('DW_LOCAL', 'True') == 'True')
    }
}

TWITTERAPI_RETRY = 3

HEALTHCHECK_TIMEOUT = os.getenv('HEALTHCHECK_TIMEOUT', 10)
HEARTBEAT_PERIOD = int(os.getenv('HEARTBEAT_PERIOD', 100))
HEALTHCHECK_REDIS_SLEEP = os.getenv('HEALTHCHECK_REDIS_SLEEP', 30)
HEALTHCHECK_KAFKA_SLEEP = os.getenv('HEALTHCHECK_KAFKA_SLEEP', 30)
HEALTHCHECK_TWITTER_SLEEP = os.getenv('HEALTHCHECK_TWITTER_SLEEP', 30)

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
