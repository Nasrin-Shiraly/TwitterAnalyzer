from pathlib import Path

from utils.db_utilities.mongodb_setup import DBConnection
import pandas as pd


class InteractionAnalysis:
    def __init__(self, _db_name, _db_url, _collection, artifacts):
        self.conn = DBConnection(_db_name, _db_url)
        self.db = self.conn.db_initiation()
        self.collection = self.db[_collection]
        self.artifacts = artifacts
        self.collection_name = _collection

    def current_nodes(self, interaction_type):
        pipeline = [{"$match": {"interaction_type": {"$in": interaction_type}}},
                    {"$project": {"interaction_type": 1, "interaction_towards": 1, "interaction_owner": 1, "_id": 0}}]
        _documents = list(self.collection.aggregate(pipeline))
        return _documents

    def draw_interaction_graph(self, interaction_type):
        columns = ['source', 'target', 'weight']
        for_gephi = pd.DataFrame(columns=columns)
        _documents = self.current_nodes(interaction_type)
        print(_documents)
        for pos, doc in enumerate(_documents):
            for_gephi.loc[pos, 'source'] = doc["interaction_owner"]
            for_gephi.loc[pos, 'target'] = doc["interaction_towards"]
            for_gephi.loc[pos, 'weight'] = doc["interaction_type"]
        analyzes_interactions = ''.join(each for each in interaction_type)
        with open(str(self.artifacts / (analyzes_interactions + '__' + self.collection_name + '__for_gephi.csv')),
                  'w') as f:
            for_gephi.to_csv(f, header=True, index=False)

    def most_interacts_with(self, user_name, interaction_type):
        pipeline = [
            {"$match": {"interaction_type": {"$in": interaction_type}, "interaction_owner": user_name.lower().strip()}},
            {"$project": {"interaction_type": 1, "interaction_towards": 1, "interaction_owner": 1, "_id": 0}}]
        _documents = list(self.collection.aggregate(pipeline))
        print(_documents)


if __name__ == '__main__':
    artifacts = Path(__file__).parent.parent / 'artifacts'
    display_account_behaviour = InteractionAnalysis(_db_name='tweet', _db_url='localhost:27017',
                                                    _collection='interaction_collection', artifacts=artifacts)
    display_account_behaviour.draw_interaction_graph(['retweet'])
    display_account_behaviour.most_interacts_with('accountID', ['reply'])
