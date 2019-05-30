from db_collection_structures.db_collections import DBCollection
from utils.utilities import formatted_utc_now
from pymongo.errors import CollectionInvalid


class Account(DBCollection):
    def __init__(self, collection, db, db_url):
        super().__init__(collection, db, db_url)

    def _collection(self):
        try:
            self.db.create_collection(self.collection)
        except CollectionInvalid:
            self.conn.db_disconnection()
            raise CollectionInvalid(f'cannot create the {self.collection}')

    def document_insert(self, account_object):
        self.db[self.collection].insert_one(

            {
                "inserted_date": formatted_utc_now(),
                "account_object": account_object,
            }
        )
