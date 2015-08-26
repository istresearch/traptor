KAFKA_HOSTS = "localhost:9092"

KAFKA_TOPIC = "traptor"

APIKEYS = {}
APIKEYS['CONSUMER_KEY'] = ""
APIKEYS['CONSUMER_SECRET'] = ""
APIKEYS['ACCESS_TOKEN'] = ""
APIKEYS['ACCESS_TOKEN_SECRET'] = ""

TRAPTOR_ID = 0
TRAPTOR_TYPE = 'follow'

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
