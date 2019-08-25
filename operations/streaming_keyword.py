import json

from db_collection_structures.tweet import Tweet
import time
from pathlib import Path
import tweepy
from utils.twitter_services.authentication import TwitterConnection


class Streamer(tweepy.StreamListener):

    def __init__(self, time_limit, _connection_handle):
        super().__init__()
        self.start_time = time.time()
        self.limit = time_limit
        self.db, self.collection, self.db_url = _connection_handle
        self.tweet_handle = Tweet(db=self.db, collection=self.collection, db_url=self.db_url)

    def on_data(self, raw_data):
        if (time.time() - self.start_time) < self.limit:
            print(raw_data)
            tweet = json.loads(raw_data.lower())
            self.tweet_handle.document_insert(tweet)
            return True
        else:
            self.tweet_handle.conn.db_disconnection()
            print('time out')
            return False

    def on_error(self, status_code):
        retry_count = 0
        if status_code == 420:
            time.sleep(15 * 60)
            retry_count += 1
            if retry_count >= 2:
                self.tweet_handle.conn.db_disconnection()
                return False


class StreamingKeyword:
    def __init__(self, _credential_file_path, keyword_, time_limit, _connection_handle):
        authentication = TwitterConnection(_credential_file_path)
        self.api = authentication.conn
        self.keyword = keyword_
        self.time_limit = time_limit
        self.connection_handle = _connection_handle

    def streamer(self):
        stream_listener = Streamer(self.time_limit, self.connection_handle)
        stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        stream.filter(track=self.keyword)


if __name__ == '__main__':
    streaming_duration = 10
    pwd = Path(__file__).parent.parent
    credential_file_path = pwd / 'credentials' / 'credentials.json'
    keyword = ['trump']
    db_alias = 'tweet'
    collection = 'twitter'
    db_url = 'localhost:27017'
    connection_handle = (db_alias, collection, db_url)

    target = StreamingKeyword(credential_file_path, keyword, streaming_duration, connection_handle)
    target.streamer()
