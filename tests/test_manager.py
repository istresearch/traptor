"""Traptor Manager API Tests"""

from traptor.manager import api

#from mock import MagicMock

class TestManagerAPI():
    
    def test_status(self):
        resp, code = api.status() 
        assert code == 200
        assert resp['status'] == 'ok'
    
    def test_validate_geo(self):
        assert True
    
    def test_validate_keyword(self):
        assert True
    
    def test_validate_userid(self):
        assert True
