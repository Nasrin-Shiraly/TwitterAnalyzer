import json
import shutil
import tempfile
from pathlib import Path
from unittest import TestCase

from data_set_preparation.keyword_analysis import KeywordAnalysis
from db_collection_structures.tweet import Tweet

import pandas as pd


class MockCollection:
    @staticmethod
    def mock_tweet_collection(dummy_tweet_pwd, tweet):
        with open(str(dummy_tweet_pwd), "r") as dummy_tweets:
            sample_tweets = json.load(dummy_tweets)

        for twt in sample_tweets:
            tweet.document_insert(twt)


class TestKeywordAnalysis(TestCase):

    def setUp(self):
        self.db_alias = 'dummyDB'
        collection = 'dummyCollection'
        db_url = 'localhost:27017'
        self.tweet_handle = Tweet(collection='dummyCollection', db='dummyDB', db_url='localhost:27017')

        mock_tweets = Path(__file__).parent / 'data' / 'tweets.json'
        self.pwd = Path(tempfile.mktemp())
        self.pwd.mkdir(parents=True, exist_ok=True)

        self.target = KeywordAnalysis(db_alias=self.db_alias, collection=collection, artifacts=self.pwd, db_url=db_url)
        mock_test_preparation = MockCollection()
        mock_test_preparation.mock_tweet_collection(mock_tweets, self.tweet_handle)

    def tearDown(self):
        self.tweet_handle.conn.db_disconnection()
        self.tweet_handle.conn.client.drop_database(self.db_alias)
        shutil.rmtree(self.pwd)

    def test_number_of_users_and_tweets(self):
        expected_result = [{'number_of_streamed_tweets': 5}]
        actual_results = self.target.number_of_users_and_tweets(1)

        self.assertEqual(actual_results[0], expected_result)
        self.assertEqual(actual_results[1], 4)
        self.assertTrue((self.pwd / "most_seen_users.json").is_file())

    def test_interactions(self):
        expected_results = [{'interaction_type': 'reply', 'interaction_owner': 'USER_2',
                             'interaction_towards': 'USER_1'}, {'interaction_type': 'retweet',
                                                                'interaction_owner': 'USER_1',
                                                                'interaction_towards': 'USER_3'}, {
                                'interaction_type': 'tweet', 'interaction_owner': 'USER_3',
                                'interaction_towards': 'USER_3'}, {'interaction_type': 'tweet',
                                                                   'interaction_owner': 'USER_2',
                                                                   'interaction_towards': 'USER_2'}, {
                                'interaction_type': 'quote', 'interaction_owner': 'USER_4',
                                'interaction_towards': 'USER_1'}]

        self.target.interactions('interaction_collection')
        actual_results = [each for each in self.target.db['interaction_collection'].find()]

        for pos in range(0, len(expected_results)):
            self.assertEqual(expected_results[pos]['interaction_type'], actual_results[pos]["interaction_type"])
            self.assertEqual(expected_results[pos]['interaction_owner'], actual_results[pos]["interaction_owner"])
            self.assertEqual(expected_results[pos]['interaction_towards'], actual_results[pos]["interaction_towards"])

    def test_redundent_tweets(self):
        most_copied_by, most_copied_from = self.target.redundant_tweet_analysis([])
        self.assertEqual(len(most_copied_by), 1)
        self.assertEqual(most_copied_by, ['USER_2'])
        self.assertEqual(most_copied_from, ['USER_3'])
        self.assertTrue((self.pwd / 'copied_tweets.csv').is_file())
        with open(str(self.pwd / 'copied_tweets.csv'), 'r') as f:
            copied_tweets = pd.read_csv(f)

        self.assertEqual(copied_tweets.loc[0, "copied_tweet"], "user_3 has tweeted")
