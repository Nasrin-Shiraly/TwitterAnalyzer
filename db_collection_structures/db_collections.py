from abc import ABC, abstractmethod

from utils.db_utilities.mongodb_setup import DBConnection


class DBCollection(ABC):
    def __init__(self, collection, db, db_url):
        self.collection = collection
        self.conn = DBConnection(db, db_url)
        self.db = self.conn.db_initiation()

        if self.collection not in self.db.list_collection_names():
            self._collection()

    @abstractmethod
    def _collection(self):
        pass

    @abstractmethod
    def document_insert(self, **kwargs):
        pass
