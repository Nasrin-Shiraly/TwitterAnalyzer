from pymongo import MongoClient


class DBConnection:
    def __init__(self, db_alias, db_url):
        self.client = MongoClient(db_url)
        self.db = self.client[db_alias]

    def db_initiation(self):
        return self.db

    def db_disconnection(self):
        self.client.close()
