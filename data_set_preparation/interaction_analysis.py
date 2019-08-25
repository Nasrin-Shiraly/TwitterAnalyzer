from pathlib import Path

from utils.db_utilities.mongodb_setup import DBConnection
import pandas as pd


class InteractionAnalysis:
    def __init__(self, _db_name, _db_url, _collection):
        self.conn = DBConnection(_db_name, _db_url)
        self.db = self.conn.db_initiation()
        self.collection = self.db[_collection]

    def current_nodes(self, interaction_type):
        pipeline = [{"$match": {"interaction_type": {"$in": interaction_type}}},
                    {"$project": {"interaction_type": 1, "interaction_towards": 1, "interaction_owner": 1, "_id": 0}}]
        _documents = list(self.collection.aggregate(pipeline))
        return _documents

    def draw_interaction_graph(self, interaction_type, gephi_csv_file_name='for_gephi.csv'):
        pwd = Path(__file__).parent.parent / 'artifacts' / gephi_csv_file_name
        columns = ['source', 'target', 'weight']
        for_gephi = pd.DataFrame(columns=columns)
        _documents = self.current_nodes(interaction_type)
        print(_documents)
        for pos, doc in enumerate(_documents):
            for_gephi.loc[pos, 'source'] = doc["interaction_owner"]
            for_gephi.loc[pos, 'target'] = doc["interaction_towards"]
            for_gephi.loc[pos, 'weight'] = doc["interaction_type"]

        with open(str(pwd), 'w') as f:
            for_gephi.to_csv(f, header=True, index=False)

    def most_interacts_with(self, user_name, interaction_type):
        pipeline = [
            {"$match": {"interaction_type": {"$in": interaction_type}, "interaction_owner": user_name.lower().strip()}},
            {"$project": {"interaction_type": 1, "interaction_towards": 1, "interaction_owner": 1, "_id": 0}}]
        _documents = list(self.collection.aggregate(pipeline))
        print(_documents)


if __name__ == '__main__':
    display_account_behaviour = InteractionAnalysis(_db_name='tweet', _db_url='localhost:27017',
                                                    _collection='interaction_collection')
    display_account_behaviour.draw_interaction_graph(['retweet'], gephi_csv_file_name='for_gephi.csv')
    display_account_behaviour.most_interacts_with('roxyinlq', ['reply'])
