from pymongo.errors import CollectionInvalid

from db_collection_structures.db_collections import DBCollection
from utils.utilities import formatted_utc_now


class Interactions(DBCollection):
    def __init__(self, collection, db, db_url):
        super().__init__(collection, db, db_url)

    def _collection(self):
        try:

            self.db.create_collection(self.collection)

        except CollectionInvalid:
            raise CollectionInvalid('cannot create the collection')

    def document_insert(self, interaction_type, interaction_date, interaction_owner, interaction_towards,
                        interaction_object=None):
        self.db[self.collection].insert_one(

            {
                "inserted_date": formatted_utc_now(),
                "interaction_date": interaction_date,
                "interaction_type": interaction_type,
                "interaction_owner": interaction_owner,
                "interaction_towards": interaction_towards,
                "interaction_object": {} if interaction_object is None else interaction_object

            }
        )
