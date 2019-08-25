import datetime
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from utils.db_utilities.mongodb_setup import DBConnection


class AccountAnalysis:
    def __init__(self, _db_name, _db_url, _collection, _user_name):
        self.conn = DBConnection(_db_name, _db_url)
        self.db = self.conn.db_initiation()
        self.collection = self.db[_collection]
        self.user_name = _user_name
        self.pwd = Path(__file__).parent.parent / 'artifacts'

    def display_user_twitter_usage_pattern(self):
        pipeline = [{"$match": {"interaction_owner": self.user_name}},
                    {"$project": {"interaction_date": 1, "_id": 0}}]
        _documents = list(self.collection.aggregate(pipeline))

        plt.style.use('ggplot')
        x_axis = [x['interaction_date'].date() for x in _documents]
        y_axis = [y['interaction_date'].time() for y in _documents]

        plt.scatter(x_axis, y_axis, xdate=True, ydate=True)

        plt.xlabel('Date')
        plt.ylabel('Time')
        plt.xticks(rotation=45)

        plt.title(self.user_name + ' Activity')
        plt.gcf().subplots_adjust(bottom=0.30)
        plt.gcf().subplots_adjust(left=0.20)
        plt.savefig(str(self.pwd / (self.user_name + '_tweet_schedule.png')), dpi=1000)
        plt.show()
        plt.close()

    def most_interacts_with(self, user_name, interaction_type):
        pipeline = [
            {"$match": {"interaction_type": {"$in": interaction_type}, "interaction_owner": user_name.lower().strip()}},
            {"$project": {"interaction_type": 1, "interaction_towards": 1, "interaction_owner": 1, "_id": 0}}]
        _documents = list(self.collection.aggregate(pipeline))
        print(_documents)

    def display_engagement(self, engagement_type, from_date=datetime.datetime.today()):
        plt.style.use('ggplot')
        pipeline = [{"$match": {"interaction_owner": self.user_name}},
                    {"$match": {"interaction_type": {"$in": engagement_type}}},
                    {"$project": {"_id": 0, "interaction_date": 1, "interaction_object.favorite_count": 1,
                                  "interaction_object.retweet_count": 1}}
                    ]

        _documents = list(self.collection.aggregate(pipeline))

        # TODO: Group by date (currently it groups by date and time) get avg of likes and rets per day. Draw.
        # TODO: Time of tweets. Activity chart.
        '''
        MongoDB query:
        
        db.getCollection('account_tweets').aggregate([{$match: {"interaction_owner": "bluejakk"}},
                    {$match: {"interaction_type": {$in: ["reply","tweet"]}}},
                    {$match: {"interaction_date": {$gt: new Date("2019-08-24 21:04:51.000Z")}}}, 
                    {$project: {  
                        "interaction_date": "$interaction_date",
                        "avg_like": {$avg: "$interaction_object.favorite_count"},
                        "avg_retweet": {$avg: "$interaction_object.retweet_count"}
                        }
                        }])
        
        '''

    #
    # like_container = self._create_dataframe('like_count')
    # like_container.reset_index(inplace=True, drop=True)
    # retweet_container = self._create_dataframe('retweet_count')
    # retweet_container.reset_index(inplace=True, drop=True)
    #
    # temp_likes = pd.Series(like_container['holder'].values, index=like_container['date'])
    # temp_retweets = pd.Series(retweet_container['holder'].values, index=retweet_container['date'])
    # temp_likes.plot(figsize=(16, 4), color='r', label="likes", legend=True)
    # temp_retweets.plot(figsize=(16, 4), color='b', label="retweets", legend=True)
    #
    # plt.xlabel('Date')
    # plt.ylabel('Time')
    # plt.xticks(rotation=45)
    # plt.title(self.user_name + ' Interactions')
    # plt.gcf().subplots_adjust(bottom=0.30)
    # plt.gcf().subplots_adjust(left=0.20)
    # plt.savefig(str(self.pwd / "final_reports" / (self.user_name + '_tweet_interactions.png')), dpi=1000)
    # plt.show()
    # plt.close()


if __name__ == '__main__':
    account_behaviour = AccountAnalysis(_db_name='twitter', _db_url='localhost:27017', _collection='account_tweets',
                                        _user_name='bluejakk')
    account_behaviour.display_engagement(['tweet', 'reply'])
