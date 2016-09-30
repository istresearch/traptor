'''
Settings for traptor
====================
'''

# Traptor defaults
TRAPTOR_TYPE = 'track'                          # Type of traptor; options: follow, track, locations
TRAPTOR_ID = 0                                  # Unique id of this traptor
RULE_CHECK_INTERVAL = 60                        # Number of second to check for Rules in Redis (while Traptor sits idle)


# Sentry URL
USE_SENTRY = 'False'                            # Use Sentry for alerting; options: True, False (in single quotes)
SENTRY_URL = ""                              # URL for Sentry if using it for application alerting

# Logging settings
LOG_LEVEL = 'INFO'                              # Default log level
LOG_DIR = '/var/log/traptor'                    # Directory to store traptor logs
LOG_FILE_NAME = 'traptor.log'                   # Name of the log file `traptor.log`

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

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
