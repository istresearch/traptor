###############################################################################
# This is a very strange handler setup, but this is how it's documented.
# See https://github.com/DataDog/datadogpy#quick-start-guide
###############################################################################
import os
from datadog import statsd, initialize

traptor_type = os.getenv('TRAPTOR_TYPE', 'track')
traptor_id = os.getenv('TRAPTOR_ID', '0')

# pseudo-CONST default tag values, if change, do so before threading starts.
DEFAULT_TAGS = [
    'traptor_type:{}'.format(traptor_type),
    'traptor_id:{}'.format(traptor_id),
]

options = {
    'statsd_host': os.getenv('STATSD_HOST_IP', 'statsd')
}
initialize(**options)

# CONST metric names and their actual dd value.
DATADOG_METRICS = {
    'heartbeat_message_sent_success': 'traptor.src.heartbeat.success.count',
    'heartbeat_message_sent_failure': 'traptor.src.heartbeat.failure.count',
    'restart_message_received': 'traptor.src.restart_message.success.count',
    'kafka_error': 'traptor.src.kafka.error',
    'redis_error': 'traptor.src.redis.error',
    'traptor_error_occurred': 'traptor.src.error.count',
    'twitter_error_occurred': 'traptor.src.twitter.error.count',
    'tweet_process_success': 'traptor.src.tweet_process.success',
    'tweet_process_failure': 'traptor.src.tweet_process.failure',
    'tweet_to_kafka_success': 'traptor.src.tweet_to_kafka.success',
    'tweet_to_kafka_failure': 'traptor.src.tweet_to_kafka.failure',
    'limit_message_received': 'traptor.src.limit.messages.count',
    'limit_message_count': 'traptor.src.limit.current_limited',
}


def increment(metric_name, tags=None):
    if tags is None:
        tags = []

    return statsd.increment(DATADOG_METRICS[metric_name], tags=DEFAULT_TAGS + tags)


def gauge(metric_name, value, tags=None):
    return statsd.gauge(DATADOG_METRICS[metric_name], value, tags=DEFAULT_TAGS + tags)
