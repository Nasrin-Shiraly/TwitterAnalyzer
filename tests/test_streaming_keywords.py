from pathlib import Path
from random import randint
from unittest import TestCase

from operations.streaming_keyword import StreamingKeyword
from utils.db_utilities.mongodb_setup import DBConnection


class TestStreamingKeyword(TestCase):
    def setUp(self) -> None:
        streaming_duration = 10
        pwd = Path(__file__).parent.parent
        credential_file_path = pwd / 'credentials' / 'credentials.json'
        keyword = ['the', 'a', 'not', 'is', 'are']
        self.db_alias = 'tweet_test' + str(randint(0, 99))
        self.collection = 'twitter_test' + str(randint(0, 99))
        db_url = 'localhost:27017'
        connection_handle = (self.db_alias, self.collection, db_url)
        self.target = StreamingKeyword(credential_file_path, keyword, streaming_duration, connection_handle)

        self.conn = DBConnection(self.db_alias, db_url)
        self.db = self.conn.db_initiation()

    def tearDown(self) -> None:
        self.conn.db_disconnection()
        self.conn.client.drop_database(self.db_alias)

    def test_streaming_keyword(self):
        self.target.streamer()
        self.assertTrue(self.collection in self.db.list_collection_names())
