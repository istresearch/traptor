KAFKA_HOSTS = "localhost:9092"
REDIS_HOST = "localhost"

KAFKA_TOPIC = "traptor"

APIKEYS = {
    'CONSUMER_KEY': "",
    'CONSUMER_SECRET': "",
    'ACCESS_TOKEN': "",
    'ACCESS_TOKEN_SECRET': ""
}

TRAPTOR_ID = 0
TRAPTOR_TYPE = 'follow'

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
