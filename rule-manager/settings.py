mysql_settings = {
    'HOST': 'localhost',
    'PORT': 3306,
    'USER': 'test',
    'PASSWD': '',
    'DB': 'test'
}

redis_settings = {
    'HOST': 'localhost',
    'PORT': 6379,
    'DB': 0
}

cooper_settings = {
    'RULES_URL': 'URL_TO_COOPER_RULE_LIST_URL'
}

kafka_settings = {
    'TOPIC_NAME': 'NAME_OF_TOPIC'
}

# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
