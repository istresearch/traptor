"""Traptor Manager API Tests"""

import os
import pytest
from mock import MagicMock
from traptor.manager import api

ONLINE = bool(os.getenv('ONLINE'))

@pytest.fixture()
def api_status():
    if os.getenv('ONLINE'):
        obj = api.status
    else:
        obj = MagicMock()
        obj.return_value = ({'status': 'ok'}, 200)
    return obj

class TestManagerAPI():

    def test_status(self, api_status):
        resp, code = api_status() 
        assert code == 200 and resp['status'] == 'ok'
    
#    def test_validate_geo(self):
#        assert True
#    
#    def test_validate_keyword(self):
#        assert True
#    
#    def test_validate_userid(self):
#        assert True
