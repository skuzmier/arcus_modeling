"""
"""

import sys
import redis
import os
import datetime

import time

from token_auth import TokenAuth
import uuid

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))


class NRGTokenPool:
    """ """

    def __init__(self, **kwargs):
        """ """
        self.r = redis.Redis(
            host=os.environ["REDISHOST"],
            port=os.environ["REDISPORT"],
            db=1,
            decode_responses=True,
        )
        self.uuid = uuid.uuid4()
        self.ttl = 20
        self.token_name = kwargs.get("token_name", "nrg_auth")

    def __del__(self):
        """ """
        if self.r:
            self.r.close()

    def create_token(self, username=None, password=None, max_users=5):
        """ """
        token = TokenAuth(username, password)
        token_name = "nrg_auth"
        doc = {
            "token": token.token,
            "max_users": str(max_users),
            "created_at": str(datetime.datetime.now()),
            "waiting_users": 0,
            'pause_all_systems': '0', # 0 = false, 1 = true, used to indicate all downstream systems should pause
            "pause": "0", # 0 = false, 1 = true, used prior to refresh
            "expires_at": str(token.expiry)
        }
        self.r.hmset(token_name, mapping=doc)
        self.r.expire(token_name, token.expires_in_seconds + 60)
        self._set_token_live_flag(token_name, token.expires_in_seconds)
        return token_name

    def is_active(self, token_name="nrg_auth"):
        """check if service is working 
        """
        return self.r.exists(token_name)

    def get_token(self, token_name="nrg_auth"):
        """ """
        # check if token exists
        if not self.is_active(token_name):
            return None
        # check if token is paused
        while self.r.hget(token_name, "pause") == "1":
            time.sleep(2)
        if self._check_max_users(token_name) > self.get_useage_count(token_name):
            self._create_useage_record(token_name)
            return self.r.hget(token_name, "token")
        else:
            self._wait_for_token(token_name)
            self._create_useage_record(token_name)
            return self.r.hget(token_name, "token")

    def get_available_tokens(self):
        """get list of available tokens when using multiple logins
        Not implemented yet
        """
        raise NotImplementedError("Not implemented yet")

    def _wait_for_token(self, token_name):
        """wait for token to be available
        """
        waiting = 0
        cnt = 0
        while self._check_max_users(token_name) > self.get_useage_count(token_name):
            time.sleep(1)
            if waiting == 0:
                waiting = 1
                self.r.hincrby(token_name, "waiting_users", 1)
            cnt += 1
            if cnt > 10:
                raise TimeoutError("Timeout waiting for token")
            if waiting == 1:
                self.r.hincrby(token_name, "waiting_users", -1)
        
    def _check_max_users(self, token_name):
        """ """
        key = f'{token_name}'
        return int(self.r.hget(key, "max_users"))

    def _create_useage_record(self, token_name):
        """ 
        """
        key = f"{token_name}_used_by_{self.uuid}"
        value = str(datetime.datetime.now())
        self.r.set(key, value)
        self.r.expire(key, self.ttl)
        self._track_duration(token_name, 'start')

    def get_useage_count(self, token_name="nrg_auth"):
        """ """
        keys = self.r.keys(f"{token_name}_used_by_*")
        return len(keys)

    def release_token(self, **kwargs):
        """delete the usage record, does not fail if expired 
        """
        token_name = kwargs.get("token_name", self.token_name)
        key = f"{token_name}_used_by_{self.uuid}"
        self.r.delete(key)
        self._track_duration(token_name, 'end')

    def _check_needs_refresh(self, token_name):
        """ check if token refresh flag exists"""
        key = f'{token_name}_live'
        return not self.r.exists(key)

    def refresh_token(self, token_name):
        """ 
        """
        #check if token not to be refreshed. Used to free up use outside of aws
        if self.r.hget(token_name, "pause_all_systems") == "1":
            self.r.set('nrg_sync_active', '0')
            return None
        #check if token exists, if not create it
        if not self.r.exists(token_name):
            self.create_token()
            return None
        if self._check_needs_refresh(token_name):
            self._refresh_token(token_name)
        return self.r.hget(token_name, "token")

    def _refresh_token(self, token_name):
        """ """
        self._pause_token(token_name)
        time.sleep(20)
        current_token = self.r.hget(token_name, "token")
        token = TokenAuth(token=current_token)
        token.release_token()
        doc = {
            "token": token.token,
            "expires_at": str(token.expiry),
            "waiting_users": 0,
            "pause": "0"
        }
        self.r.hmset(token_name, doc)
        self.r.expire(token_name, token.expires_in_seconds + 60)
        self._set_token_live_flag(token_name, token.expires_in_seconds)

    def _pause_token(self, token_name):
        """ """
        self.r.hset(token_name, "pause", "1")

    def _set_token_live_flag(self, token_name, expires_in_seconds):
        """ sets a flag to indicate the token is live
            with an expiry time 30 seconds before the token expires
            Refresh task needs to run every 15 seconds to keep token alive
        """
        key = f'{token_name}_live'
        value = "True"
        self.r.set(key, value)
        self.r.expire(key, expires_in_seconds - 30)
    
    def _track_duration(self, token_name, action):
        """track duration of token used by this instance 
        """
        key = f"{token_name}_duration_{self.uuid}"
        if action == 'start':
            value = {"start": str(datetime.datetime.now()),
                     "end": ''}
            self.r.hmset(key, mapping=value)
        elif action == 'end':
            self.r.hset(key, "end", str(datetime.datetime.now()))
        self.r.expire(key, 300)
    
    def get_durations(self, token_name):
        """get durations of token usage
        """
        keys = self.r.keys(f"{token_name}_duration_*")
        timings = [self.r.hgetall(key) for key in keys]
        for x in timings:
            x['start'] = datetime.datetime.strptime(x['start'], "%Y-%m-%d %H:%M:%S.%f")
            if x['end']:
                x['end'] = datetime.datetime.strptime(x['end'], "%Y-%m-%d %H:%M:%S.%f")
            x['duration'] = x['end'] - x['start'] if x['end'] else None
        return timings

if __name__ == '__main__':
    token_pool = NRGTokenPool()
    token_name = 'nrg_auth'
    token_name = token_pool.create_token()
    consecutive_errors = 0
    while True:
        try:
            time.sleep(15)
            token_pool.refresh_token(token_name)
            consecutive_errors = 0
        except KeyboardInterrupt:
            break
        except Exception as e:
            consecutive_errors += 1
            current_time = datetime.datetime.now()
            print(f"{current_time}: {e}, consecutive errors: {consecutive_errors}")
            time.sleep(30 * consecutive_errors)
            if consecutive_errors > 10:
                break
    print("Exiting")

