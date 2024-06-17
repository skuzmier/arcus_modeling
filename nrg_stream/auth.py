import json
import datetime
import requests
import os

import sys
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/nrg_token_pool"))

from token_pool import NRGTokenPool


class NRGAuth:
    """
    """

    def __init__(self, username=None, password=None):
        """
        """
        self.username = username
        self.password = password
        self.server = 'api.nrgstream.com'
        self.token_path = '/api/security/token'
        self.release_path = '/api/ReleaseToken'
        self.token_pool = None
        self.token_payload = None
        self.expiry = None
        self._token = None
        self.pool_mode = False
        self._setup()
    
    def _setup(self):
        """
        """
        if not self._setup_token_pool():
            self._setup_login()
    
    def _setup_token_pool(self) -> bool:
        """try to get the token pool from the NRG Token Pool package
        """
        try:
            self.token_pool = NRGTokenPool()
            if self.token_pool.is_active():
                self.pool_mode = True
                return True
            else:
                return False
        except Exception as e:
            return False

    def _setup_login(self):
        """validate username and password were passed at instantiation
            it not search for them in env vars, then in a credentials.txt file, then in a credentials.json file
        """
        if self.username is None or self.password is None:
            if 'NRGSTREAM_USERNAME' in os.environ and 'NRGSTREAM_PASSWORD' in os.environ:
                self.username = os.environ['NRGSTREAM_USERNAME']
                self.password = os.environ['NRGSTREAM_PASSWORD']
            elif os.path.exists('credentials.txt'):
                with open('credentials.txt', 'r') as f:
                    self.username = f.readline().split(',')[0]
                    self.password = f.readline().split(',')[1]
            elif os.path.exists('credentials.json'):
                with open('credentials.json', 'r') as f:
                    self.username = json.load(f)['username']
                    self.password = json.load(f)['password']
            else:
                raise ValueError('username and password must be passed at instantiation or in env vars or in a credentials.txt or credentials.json file')
        
        self.token_payload = f'grant_type=password&username={self.username}&password={self.password}'
    
    @property
    def token(self):
        """
        """
        if self._token is None:
            self._get_token()
        if self.expiry < datetime.datetime.now() + datetime.timedelta(seconds=5):
            self._get_token()
        return self._token
    
    def _get_token(self):
        """get the token from NRG Stream using requests package
        """
        if self.pool_mode:
            self._get_token_from_pool()
            return None
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        response = requests.post(f'https://{self.server}{self.token_path}', 
                                 data=self.token_payload, headers=headers)
        if response.status_code == 200:
            self._token = response.json()['access_token']
            self.expiry = datetime.datetime.now() + datetime.timedelta(seconds=response.json()['expires_in'])
        else:
            raise ValueError(f'failed to get token from NRG Stream: {response.status_code} - {response.reason}')
    
    def release_token(self):
        """
        """
        if self._token is None:
            return True
        if self.pool_mode:
            self.token_pool.release_token()
            self._token = None
            self.expiry = None
            return True
        if self.expiry < datetime.datetime.now():
            return True
        headers = {}
        headers['Authorization'] = f'Bearer {self.token}'
        response = requests.delete(f'https://{self.server}{self.release_path}', headers=headers)
        if response.status_code == 200:
            self._token = None
            self.expiry = None
        else:
            raise ValueError(f'failed to release token from NRG Stream: {response.status_code} - {response.reason}')
    
    def _get_token_from_pool(self):
        """get the token from the token pool
        """
        self._token = self.token_pool.get_token()
        self.expiry = datetime.datetime.now() + datetime.timedelta(seconds=self.token_pool.ttl)
        return None
        


class BearerAuth(requests.auth.AuthBase):
    """Attaches HTTP Bearer Authentication to the given Request object.
    """
    def __init__(self, token):
        """
        """
        self.token = token
    
    def __call__(self, r):
        """modify the request to include the bearer token
        """
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r