from unittest import TestCase

from db_collection_structures.account import Account
from db_collection_structures.interaction import Interaction
from db_collection_structures.interaction_between_accounts import InteractionBetweenAccounts
from db_collection_structures.tweet import Tweet
from utils.utilities import formatted_created_at, formatted_utc_now


class TestInteraction(TestCase):
    def setUp(self) -> None:
        self.collection = 'DCollection'
        self.db = 'DDB'
        self.db_url = 'localhost:27017'
        self._handle = Interaction(collection='DCollection', db='DDB', db_url='localhost:27017')

    def tearDown(self) -> None:
        self._handle.conn.db_disconnection()
        self._handle.conn.client.drop_database(self.db)

    def test_interaction(self):
        expected_result = {"interaction_type": "retweet", "interaction_owner": "me",
                           "interaction_towards": "you",
                           "interaction_date": formatted_created_at("thu may 30 13:45:20 +0000 2019")
                           }
        self._handle.document_insert(interaction_type='retweet', interaction_owner='me',
                                     interaction_towards='you',
                                     interaction_date=formatted_created_at("thu may 30 13:45:20 +0000 2019"))
        actual_result = []
        for each in self._handle.conn.client[self.db][self.collection].find():
            actual_result.append(each)

        self.assertEqual(actual_result[0]['interaction_owner'], expected_result['interaction_owner'])


class TestAccount(TestCase):
    def setUp(self) -> None:
        self.collection = 'DCollection'
        self.db = 'DDB'
        self.db_url = 'localhost:27017'
        self._handle = Account(collection='DCollection', db='DDB', db_url='localhost:27017')

    def tearDown(self) -> None:
        self._handle.conn.db_disconnection()
        self._handle.conn.client.drop_database(self.db)

    def test_account(self):
        expected_result = {
            "inserted_date": formatted_utc_now(),
            "account_object": {"dummyLine": "someVal", "secondLine": "secondVal"},
        }
        self._handle.document_insert(account_object={"dummyLine": "someVal", "secondLine": "secondVal"})
        actual_result = []
        for each in self._handle.conn.client[self.db][self.collection].find():
            actual_result.append(each)

        self.assertEqual(actual_result[0]['account_object'], expected_result['account_object'])


class TestTweet(TestCase):
    def setUp(self) -> None:
        self.collection = 'DCollection'
        self.db = 'DDB'
        self.db_url = 'localhost:27017'
        self._handle = Tweet(collection='DCollection', db='DDB', db_url='localhost:27017')

    def tearDown(self) -> None:
        self._handle.conn.db_disconnection()
        self._handle.conn.client.drop_database(self.db)

    def test_account(self):
        inserted_date = formatted_utc_now()
        expected_result = {
            "tweet_object": {"dummyLine": "someVal", "secondLine": "secondVal"},
        }
        self._handle.document_insert(tweet_object={"dummyLine": "someVal", "secondLine": "secondVal"})
        actual_result = []
        for each in self._handle.conn.client[self.db][self.collection].find():
            actual_result.append(each)

        self.assertEqual(actual_result[0]['tweet_object'], expected_result['tweet_object'])
        self.assertAlmostEqual(actual_result[0]["inserted_date"], inserted_date)


class TestInteractionBetweenAccounts(TestCase):
    def setUp(self) -> None:
        self.collection = 'DCollection'
        self.db = 'DDB'
        self.db_url = 'localhost:27017'
        self._handle = InteractionBetweenAccounts(collection='DCollection', db='DDB',
                                                  db_url='localhost:27017')

    def tearDown(self) -> None:
        self._handle.conn.db_disconnection()
        self._handle.conn.client.drop_database(self.db)

    def test_interaction(self):
        expected_result = {"interaction_type": "tweet", "interaction_owner": "you",
                           "interaction_object": {"someval": "someval"},
                           }
        self._handle.document_insert(interaction_type='tweet',
                                     interaction_owner='you',
                                     interaction_object={"someval": "someval"})
        actual_result = []
        for each in self._handle.conn.client[self.db][self.collection].find():
            actual_result.append(each)

        self.assertEqual(actual_result[0]['interaction_type'], expected_result['interaction_type'])
