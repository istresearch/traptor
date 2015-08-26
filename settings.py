KAFKA_HOSTS = "localhost:9092"

KAFKA_TOPIC = "traptor"

APIKEYS[CONSUMER_KEY] = ""
APIKEYS[CONSUMER_SECRET] = ""
APIKEYS[ACCESS_TOKEN] = ""
APIKEYS[ACCESS_TOKEN_SECRET] = ""

TRACK_RULES = []

FOLLOW_RULES = []

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
