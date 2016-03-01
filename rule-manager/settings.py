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
    'RULES_URL': 'COOPER_URL_HERE'
}

sentry_settings = {
    'USE_SENTRY': FALSE,
    'SENTRY_URL': 'SENTRY_URL_HERE'
}

rule_refresh_settings = {
    'RULE_REFRESH_TIME': 600
}

traptor_pubsub_settings = {
    'CHANNEL_NAME': 'CHANNEL_NAME_HERE'
}

log_settings = {
    'LOG_LEVEL': 'INFO'
}


# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
