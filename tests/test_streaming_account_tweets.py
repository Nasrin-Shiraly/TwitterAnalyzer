from unittest import TestCase

from db_collection_structures.interactions import Interactions


class TestInteraction(TestCase):
    def setUp(self) -> None:
        self.collection = 'DCollection'
        self.db = 'DDB'
        self.db_url = 'localhost:27017'
        self._handle = Interactions(collection='DCollection', db='DDB', db_url='localhost:27017')

    def tearDown(self) -> None:
        self._handle.conn.db_disconnection()
        self._handle.conn.client.drop_database(self.db)

    def test_interaction(self):
        pass
    '''
    This functionality is live and cannot be tested
    
    '''