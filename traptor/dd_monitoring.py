###############################################################################
# This is a very strange handler setup, but this is how it's documented.
# See https://github.com/DataDog/datadogpy#quick-start-guide
###############################################################################
import os
from datadog import initialize
options = {
    'statsd_host': os.getenv('STATSD_HOST_IP', '127.0.0.1')
}
initialize(**options)

from datadog import statsd
DATADOG_METRICS = {
    'tweet_process_success': 'traptor.src.tweet_process.success',
    'tweet_process_failure': 'traptor.src.tweet_process.failure',
    'tweet_to_kafka_success': 'traptor.src.tweet_to_kafka.success',
    'tweet_to_kafka_failure': 'traptor.src.tweet_to_kafka.failure',
}

def increment(metric_name):
    return statsd.increment(DATADOG_METRICS[metric_name])

def gauge(metric_name, value):
    return statsd.gauge(DATADOG_METRICS[metric_name], value)
