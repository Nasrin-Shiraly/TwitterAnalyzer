import json
import tweepy


class TwitterConnection:
    def __init__(self, credential_file):
        self.credential_file_path = credential_file
        self._credentials = self.credentials
        self._api = self.conn

    @property
    def credentials(self):
        with open(str(self.credential_file_path), 'r') as f:
            self._credentials = json.load(f)
        return self._credentials

    @property
    def conn(self):
        api_key = self._credentials["api_key"]
        api_secret_key = self._credentials["api_secret_key"]
        access_token = self._credentials["access_token"]
        access_token_secret = self._credentials["access_token_secret"]
        auth = tweepy.OAuthHandler(api_key, api_secret_key)
        auth.set_access_token(access_token, access_token_secret)
        self._api = tweepy.API(auth)
        return self._api
