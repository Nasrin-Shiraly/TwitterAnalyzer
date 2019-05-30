import json

from db_collection_structures.interaction import Interaction
from utils.utilities import formatted_created_at
from utils.db_utilities.mongodb_setup import DBConnection


class KeywordAnalysis:
    def __init__(self, db_alias, db_url, collection, artifacts_path):
        self.pwd = artifacts_path
        self.conn = DBConnection(db_alias, db_url)
        self.db = self.conn.db_initiation()

        self.db_name = db_alias
        self.db_url = db_url
        self.collection = collection

    def number_of_users_and_tweets(self, threshold):
        cursor = self.db[self.collection]

        pipeline_ids = [{'$group': {'_id': '$tweet_object.user.id_str', 'count': {'$sum': 1}}},
                        {"$sort": {'count': -1}}]

        pipeline_names = [{'$group': {'_id': '$tweet_object.user.screen_name', 'count': {'$sum': 1}}},
                          {"$sort": {'count': -1}}]
        pipeline_number_of_tweets = [{'$count': "number_of_streamed_tweets"}]

        query_ids = list(cursor.aggregate(pipeline_ids))
        query_names = list(cursor.aggregate(pipeline_names))
        number_of_tweets = list(cursor.aggregate(pipeline_number_of_tweets))
        len_unique_ids = len(set([twitter_id['_id'] for twitter_id in query_ids]))
        user_ids_repeated_more_than_threshold = [{'id': each['_id'], 'count': each['count']} for each in query_ids if
                                                 each['count'] > threshold]
        user_names_repeated_more_than_threshold = [{'name': each['_id'], 'count': each['count']} for each in query_names
                                                   if each['count'] > threshold]

        id_and_names_with_repetitions = [{'id': user_id['id'], 'screen_name': name['name'], 'count': user_id['count']}
                                         for pos, user_id in enumerate(user_ids_repeated_more_than_threshold) for
                                         position, name in enumerate(user_names_repeated_more_than_threshold) if
                                         pos == position]

        with open(self.pwd / 'most_seen_users.json', 'w') as report:
            json.dump(id_and_names_with_repetitions, report)

        del user_ids_repeated_more_than_threshold, user_names_repeated_more_than_threshold, \
            id_and_names_with_repetitions
        return number_of_tweets, len_unique_ids

    def interactions(self, collection):

        req_fields = {"tweet_object.user.screen_name": 1, "tweet_object.created_at": 1,
                      "tweet_object.retweeted_status.user.screen_name": 1,
                      "tweet_object.quoted_status.user.screen_name": 1,
                      "tweet_object.in_reply_to_screen_name": 1,
                      }
        cursor = self.db[self.collection].find({}, req_fields)

        interaction = Interaction(collection=collection, db=self.db_name, db_url=self.db_url)
        for pos, item in enumerate(cursor):
            print(f"tweet number {pos}")
            account = item["tweet_object"]['user']['screen_name']
            created_at = formatted_created_at(item["tweet_object"]['created_at'])

            try:
                retweeted_account = item["tweet_object"]["retweeted_status"]['user']['screen_name']
                interaction.document_insert(interaction_type="retweet", interaction_owner=account,
                                            interaction_towards=retweeted_account, interaction_date=created_at)
            except KeyError:
                try:
                    quoted_account = item["tweet_object"]["quoted_status"]['user']['screen_name']
                    interaction.document_insert(interaction_date=created_at, interaction_owner=account,
                                                interaction_towards=quoted_account, interaction_type='quote')
                except KeyError:
                    if item["tweet_object"]["in_reply_to_screen_name"]:
                        replied_account = item["tweet_object"]["in_reply_to_screen_name"]
                        interaction.document_insert(interaction_date=created_at, interaction_owner=account,
                                                    interaction_towards=replied_account, interaction_type='reply')
                    else:
                        interaction.document_insert(interaction_date=created_at, interaction_owner=account,
                                                    interaction_towards=account, interaction_type='tweet')
        if cursor:
            del cursor
