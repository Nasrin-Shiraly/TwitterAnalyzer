import json
from collections import Counter
import pandas as pd
from past.builtins import xrange

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

        length = len(user_names_repeated_more_than_threshold)

        id_and_names_with_repetitions = [{'id': user_ids_repeated_more_than_threshold[pos]['id'],
                                          'screen_name': user_names_repeated_more_than_threshold[pos]['name'],
                                          'count': user_ids_repeated_more_than_threshold[pos]['count']}
                                         for pos in xrange(0, length)]

        print(id_and_names_with_repetitions)

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

    def redundant_tweet_analysis(self, excluded_list):
        req_fields = {'tweet_object.user.screen_name': 1,
                      "tweet_object.retweeted_status.user.screen_name": 1,
                      "tweet_object.text": 1, 'tweet_object.timestamp_ms': 1,
                      }
        cursor = self.db[self.collection].find({}, req_fields)
        main_cursor = self.db[self.collection]
        columns = ['account', 'copied_tweet', 'copy_from_account']
        copied_tweets = pd.DataFrame(columns=columns)
        index = 0
        for pos, item in enumerate(cursor):
            print(f"analyzing redundant tweets for the tweet number: {pos}")
            account = item["tweet_object"]['user']['screen_name']
            text = item["tweet_object"]['text']
            if text.lower() not in excluded_list:
                try:
                    _ = item["tweet_object"]["retweeted_status"]['user']['screen_name']
                except KeyError:
                    pipeline = [{"$match": {"tweet_object.text": {"$regex": "^(?!RT @)"}}},
                                {"$match": {"tweet_object.text": {"$eq": text}}},
                                {"$match": {"tweet_object.timestamp_ms": {
                                    "$lt": item["tweet_object"]["timestamp_ms"]}}},
                                {"$sort": {"tweet_object.timestamp_ms": 1}},
                                {"$limit": 1}]

                    query = list(main_cursor.aggregate(pipeline))
                    if query:
                        index += 1
                        copied_tweets.loc[index, "account"] = account
                        copied_tweets.loc[index, "copied_tweet"] = text
                        copied_tweets.loc[index, "copy_from_account"] = str(
                            query[0]["tweet_object"]["user"]["screen_name"]).replace(r"\n", " ")
                        print(f"{account} has coppied {copied_tweets.loc[index, 'copy_from_account']}")
            else:
                print(f"{text} will not be analyzed as it's in {excluded_list}")

        if cursor:
            del cursor

        print('done!')

        most_copied_by = sorted(Counter(copied_tweets["account"]), key=lambda x: x[1], reverse=True)
        most_copied_from = sorted(Counter(copied_tweets["copy_from_account"]), key=lambda x: x[1], reverse=True)
        with open(str(self.pwd / 'copied_tweets.csv'), 'w') as f:
            copied_tweets.to_csv(f)
        return most_copied_by, most_copied_from
