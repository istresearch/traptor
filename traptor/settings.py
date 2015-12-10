'''
Settings for traptor
====================
'''
LOG_LEVEL = 'INFO'

KAFKA_HOSTS = "localhost:9092"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Kafka topic to write all twitter data
KAFKA_TOPIC = "traptor"

# Your API information.  Fill this out in localsettings.py!
APIKEYS = {
    'CONSUMER_KEY': "",
    'CONSUMER_SECRET': "",
    'ACCESS_TOKEN': "",
    'ACCESS_TOKEN_SECRET': ""
}

'''
Each 'traptor_type' has a unqiue 'traptor_id'.  This ID is how traptor knows
where to look for a ruleset in Redis.  For example, traptor-follow:0
'''

# Options for TRAPTOR_TYPE:  follow, track
TRAPTOR_TYPE = 'track'
TRAPTOR_ID = 0

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
