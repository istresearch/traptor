"""Traptor Manager API Tests"""

import os
import pytest
from mock import MagicMock
from traptor.manager import api

ONLINE = bool(os.getenv('ONLINE'))

VALID_USER_USERNAME = 'BarackObama'
VALID_USER_USERID = '813286'

VALID_KEYWORD = 'news'
VALID_KEYWORD_THRESHOLD = 2.0

INVALID_KEYWORD = 'sakfjksdjfhlaksf;lkjsaf;lksajfd;lkjsafdlkjhasdf'
INVALID_KEYWORD_THRESHOLD = 0.5

VALID_GEO_LEGACY = '80,80,80,80'

INVALID_GEO_LEGACY = '100,100,100,100'
INVALID_GEO_LEGACY_ERROR = 'Lat/lon out of range: 100, 100'

VALID_GEO_JSON = { 
    "type": "Feature", 
    "geometry": { 
        "type": "Polygon", 
        "coordinates": [[[100.0, -80.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]
    }
}

INVALID_GEO_JSON = { 
    "type": "Feature", 
    "geometry": { 
        "type": "Polygon", 
        "coordinates": [[[100.0, -100.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]
    }
}
INVALID_GEO_JSON_ERROR = 'Lat/lon out of range: -100.0, 100.0'

@pytest.fixture()
def api_valid_username():
    if os.getenv('ONLINE'):
        obj = api.validate
    else:
        obj = MagicMock()
        obj.return_value = ({'status': 'ok', 'userid': VALID_USER_USERID }, 200)
    return obj

@pytest.fixture()
def api_valid_keyword():
    if os.getenv('ONLINE'):
        obj = api.validate
    else:
        obj = MagicMock()
        obj.return_value = ({'status': 'ok', 'tweets_per_second': VALID_KEYWORD_THRESHOLD}, 200)
    return obj

@pytest.fixture()
def api_invalid_keyword():
    if os.getenv('ONLINE'):
        obj = api.validate
    else:
        obj = MagicMock()
        obj.return_value = ({'status': 'ok', 'tweets_per_second': INVALID_KEYWORD_THRESHOLD}, 200)
    return obj

@pytest.fixture()
def api_geo():
    obj = api.validate
    return obj

class TestManagerAPI():

    def test_valid_username(self, api_valid_username):
        resp, code = api_valid_username({'type': 'username', 'value': VALID_USER_USERNAME})
        assert code == 200 and resp['status'] == 'ok' and resp['userid'] == VALID_USER_USERID
    
    def test_valid_username_with_prefix(self, api_valid_username):
        resp, code = api_valid_username({'type': 'username', 'value': '@{}'.format(VALID_USER_USERNAME)})
        assert code == 200 and resp['status'] == 'ok' and resp['userid'] == VALID_USER_USERID
    
    def test_valid_keyword(self, api_valid_keyword):
        resp, code = api_valid_keyword({'type': 'keyword', 'value': VALID_KEYWORD})
        assert code == 200 and resp['status'] == 'ok' and resp['tweets_per_second'] >= VALID_KEYWORD_THRESHOLD
    
    def test_valid_geo_json(self, api_geo):
        resp, code = api_geo({'type': 'geo', 'value': VALID_GEO_JSON})
        assert code == 200 and resp['status'] == 'ok' and resp['geo_type'] == 'geo_json'

    def test_invalid_geo_json(self, api_geo):
        resp, code = api_geo({'type': 'geo', 'value': INVALID_GEO_JSON})
        assert code == 500 and resp['status'] == 'error' and resp['error'] == INVALID_GEO_JSON_ERROR
    
    def test_valid_geo_legacy(self, api_geo):
        resp, code = api_geo({'type': 'geo', 'value': VALID_GEO_LEGACY})
        assert code == 200 and resp['status'] == 'ok' and resp['geo_type'] == 'legacy'

    def test_invalid_geo_legacy(self, api_geo):
        resp, code = api_geo({'type': 'geo', 'value': INVALID_GEO_LEGACY})
        assert code == 500 and resp['status'] == 'error' and resp['error'] == INVALID_GEO_LEGACY_ERROR

