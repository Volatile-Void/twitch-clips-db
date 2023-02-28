import datetime
import json
import time
from urllib import request, parse

class TwitchAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_valid_until = None
        self.ratelimit_remaining = -1
        self.ratelimit_reset = None
        self.user_id_cache = {}

    def get_token(self):
        now = datetime.datetime.utcnow()
        if self.token_valid_until is None or now >= self.token_valid_until:
            token, valid_seconds = self._request_token()
            self.token = token
            self.token_valid_until = now + datetime.timedelta(seconds=valid_seconds)
        return self.token

    def _request_token(self):
        url = 'https://id.twitch.tv/oauth2/token'
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials',
        }
        resp = self._api_request(url, data=data, no_auth=True)
        return resp['access_token'], resp['expires_in']

    def _api_request(self, url, params=None, data=None, no_auth=False):
        encoded_params = ''
        encoded_data = None
        if params:
            encoded_params = '?%s' % parse.urlencode(params, True)
        if data:
            encoded_data = parse.urlencode(data, True).encode()
        req = request.Request(url + encoded_params, data=encoded_data)
        if self.ratelimit_remaining == 0:
            seconds_until_refresh = (self.ratelimit_refresh - datetime.datetime.utcnow()).total_seconds()
            time.sleep(seconds_until_refresh)
        if not no_auth:
            req.add_header('Client-Id', self.client_id)
            req.add_header('Authorization', 'Bearer %s' % self.get_token())
        with request.urlopen(req) as resp:
            if remaining := resp.headers.get('Ratelimit-Remaining'):
                self.ratelimit_remaining = int(remaining)
            if reset := resp.headers.get('Ratelimit-Reset'):
                self.ratelimit_reset = datetime.datetime.utcfromtimestamp(int(reset))
            return json.load(resp)

    @staticmethod
    def to_rfc3339(dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def get_user_info(self, ids=[], login_names=[]):
        url = 'https://api.twitch.tv/helix/users'
        params = {'id': ids, 'login': login_names}
        users = self._api_request(url, params)['data']
        for user in users:
            self.user_id_cache[user['login']] = user['id']
        return users

    def get_user_id(self, login_name):
        user_id = self.user_id_cache.get(login_name)
        if user_id is None:
            self.get_user_info(login_names=login_name)
            user_id = self.user_id_cache.get(login_name)
        return user_id

    def get_game_info(self, ids=[], names=[], igdb_ids=[]):
        url = 'https://api.twitch.tv/helix/games'
        params = {'id': ids, 'name': names, 'igdb_id': igdb_ids}
        games = self._api_request(url, params)['data']
        return games

    def get_broadcaster_clips(self, broadcaster_id, max_count=-1, start_date=None, end_date=None):
        url = 'https://api.twitch.tv/helix/clips'
        yielded_clips = 0
        if max_count == 0:
            return
        batch_size = min(100, max_count)
        if batch_size < 0:
            batch_size = 100
        params = {'broadcaster_id': broadcaster_id, 'first': batch_size}
        if start_date is not None:
            params['started_at'] = self.to_rfc3339(start_date)
        if end_date is not None:
            params['ended_at'] = self.to_rfc3339(end_date)
        while True:
            resp = self._api_request(url, params)
            data = resp['data']
            page = resp['pagination']
            yielded_clips += len(data)
            yield from data
            if not page or (max_count > 0 and yielded_clips >= max_count):
                break
            if max_count > 0:
                batch_size = min(100, max_count - yielded_clips)
                params['first'] = batch_size
            params['after'] = page['cursor']
