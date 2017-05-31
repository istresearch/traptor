"""Traptor Manager API Tests"""

import os
from mock import MagicMock, patch
from traptor.manager import api

ONLINE = bool(os.getenv('ONLINE') == 'True')

class TestManagerAPI():
    
    def test_status(self):
        if ONLINE:
            _api_status = api.status
        else:
            _api_status = MagicMock()
            _api_status.return_value = ({'status': 'ok'}, 200)
        resp, code = _api_status() 
        assert code == 200
        assert resp['status'] == 'ok'
    
#    def test_validate_geo(self):
#        assert True
#    
#    def test_validate_keyword(self):
#        assert True
#    
#    def test_validate_userid(self):
#        assert True
