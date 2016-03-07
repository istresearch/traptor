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
    'RULES_URL': 'COOPER_URL_HERE',
    'TEST_DB_HOST': 'TEST_DB_HOST_HERE',
    'TEST_DB_PORT': 3306,
    'TEST_DB_USER': 'TEST_DB_USER_HERE',
    'TEST_DB_PASSWORD': 'TEST_DB_PASSWORD_HERE',
    'TEST_DB': 'TEST_DB_NAME_HERE'
}

sentry_settings = {
    'USE_SENTRY': 'False',
    'SENTRY_URL': 'SENTRY_URL_HERE'
}

rule_refresh_settings = {
    'RULE_REFRESH_TIME': 600
}

traptor_pubsub_settings = {
    'CHANNEL_NAME': 'CHANNEL_NAME_HERE'
}


# Local Overrides
# ~~~~~~~~~~~~~~~

try:
    from localsettings import *
except ImportError:
    pass
