'''
Settings for traptor
====================
'''
SENTRY_SECRET = ""
LOG_LEVEL = 'INFO'

KAFKA_HOSTS = "localhost:9092"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

'''
In order for a Traptor to restart itself, it looks for a message in
Redis in the format of <traptor-type>:<traptor-id>. Once it sees a
message for itself it restarts, thus refreshing it's rules.
'''
REDIS_PUBSUB_CHANNEL = "traptor-notify"

# Kafka topic to write all twitter data
KAFKA_TOPIC = "traptor"

# Your API information.  Fill this out in localsettings.py!
APIKEYS = (
           {
            'CONSUMER_KEY': "",
            'CONSUMER_SECRET': "",
            'ACCESS_TOKEN': "",
            'ACCESS_TOKEN_SECRET': ""
            }
)

'''
Each 'traptor_type' has a unqiue 'traptor_id'.  This ID is how traptor knows
where to look for a ruleset in Redis.  For example, traptor-follow:0:5 is
a traptor-follow instance, with traptor_id = 0, and the rule_id = 5.
'''

# Options for TRAPTOR_TYPE:  follow, track, locations
TRAPTOR_TYPE = ''
TRAPTOR_ID = 0

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
