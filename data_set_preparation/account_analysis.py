from pathlib import Path
import matplotlib.pyplot as plt
from utils.db_utilities.mongodb_setup import DBConnection
from utils.utilities import date_of_n_utc_date_ago, days_between_dates


class AccountAnalysis:
    def __init__(self, _db_name, _db_url, _collection, _user_name, artifacts):
        self.conn = DBConnection(_db_name, _db_url)
        self.db = self.conn.db_initiation()
        self.collection = self.db[_collection]
        self.user_name = _user_name.lower().strip()
        self.artifact = artifacts
        self.pwd = Path(__file__).parent.parent / 'artifacts'

    def account_details(self):
        pipeline = [
            {
                "$match": {
                    "interaction_owner": self.user_name
                }
            },
            {
                "$sort": {
                    "interaction_date": -1
                }
            },
            {
                "$project": {
                    "summary_date": "$interaction_date",
                    "screen_name": "$interaction_object.user.screen_name",
                    "created_at": "$interaction_object.user.created_at",
                    "followers_count": "$interaction_object.user.followers_count",
                    "following_count": "$interaction_object.user.friends_count",
                    "list_count": "$interaction_object.user.listed_count",
                    "likes_count": "$interaction_object.user.favourites_count",
                    "tweet_count": "$interaction_object.user.statuses_count"
                }
            },
            {
                "$limit": 1
            }
        ]
        _documents = list(self.collection.aggregate(pipeline))
        return _documents

    def number_of_interactions(self, interaction_type, days_ago):
        pipeline = [
            {
                "$match": {
                    "interaction_date": {
                        "$gt": date_of_n_utc_date_ago(days_ago)
                    }
                }
            },
            {
                "$match": {
                    "interaction_owner": self.user_name
                }
            },
            {
                "$match": {
                    "interaction_type": {
                        "$in": interaction_type
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "interaction_type": 1
                }
            },
            {
                "$group": {
                    "_id": "$interaction_type",
                    "count": {
                        "$sum": 1
                    }
                }
            }
        ]

        _documents = list(self.collection.aggregate(pipeline))
        return _documents

    def most_interacts_with(self, interaction_type):
        pipeline = [
            {
                "$match": {
                    "interaction_owner": self.user_name
                }
            },
            {
                "$match": {
                    "interaction_type": interaction_type
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "interaction_towards": 1
                }
            },
            {
                "$group": {
                    "_id": "$interaction_towards",
                    "count": {
                        "$sum": 1
                    }
                }
            },
            {"$sort": {'count': -1}}
        ]
        _documents = list(self.collection.aggregate(pipeline))
        return _documents

    def display_engagement(self, engagement_type, days_ago):
        plt.style.use('ggplot')
        pipeline = [{"$match": {"interaction_owner": self.user_name}},
                    {"$match": {"interaction_date": {"$gt": date_of_n_utc_date_ago(days_ago)}}},
                    {"$match": {"interaction_type": {"$in": engagement_type}}},

                    {"$project": {
                        "interaction_date": {"$substr": [{"$toString": "$interaction_date"}, 0, 10]},
                        "likes": "$interaction_object.favorite_count",
                        "retweets": "$interaction_object.retweet_count"
                    }
                    },
                    {"$group":
                        {
                            "_id": "$interaction_date",
                            "avg_likes": {"$avg": "$likes"},
                            "avg_retweets": {"$avg": "$retweets"}
                        }

                    }]

        account_engagement_documents = list(self.collection.aggregate(pipeline))
        print(account_engagement_documents)
        pipeline = [
            {
                "$match": {
                    "interaction_owner": self.user_name
                }
            },
            {
                "$match": {
                    "interaction_type": {
                        "$in": engagement_type
                    }
                }
            },
            {
                "$project": {
                    "interaction_date": 1,
                    "_id": 0
                }
            }
        ]
        account_activity_documents = list(self.collection.aggregate(pipeline))
        print(account_activity_documents)

        # TODO: Time of tweets. Activity chart.

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

        # plt.style.use('ggplot')
        # x_axis = [x['interaction_date'].date() for x in _documents]
        # y_axis = [y['interaction_date'].time() for y in _documents]
        #
        # plt.scatter(x_axis, y_axis, xdate=True, ydate=True)
        #
        # plt.xlabel('Date')
        # plt.ylabel('Time')
        # plt.xticks(rotation=45)
        #
        # plt.title(self.user_name + ' Activity')
        # plt.gcf().subplots_adjust(bottom=0.30)
        # plt.gcf().subplots_adjust(left=0.20)
        # plt.savefig(str(self.pwd / (self.user_name + '_tweet_schedule.png')), dpi=1000)
        # plt.show()
        # plt.close()
        return account_activity_documents, account_engagement_documents

    def account_summary(self):
        account_activity_documents, account_engagement_documents = account_behaviour.display_engagement(['tweet'], 10)
        most_quotes = self.most_interacts_with('quote')
        most_replies_to = self.most_interacts_with('reply')
        most_retweets = self.most_interacts_with('retweet')
        number_of_retweets = self.number_of_interactions(['retweet'], 100)
        number_of_quotes = self.number_of_interactions(['quote'], 100)
        number_of_replies = self.number_of_interactions(['reply'], 100)
        number_of_tweets = self.number_of_interactions(['tweet'], 100)
        total_engagement = self.number_of_interactions(['tweet', 'reply', 'quote', 'retweet'], 100)
        account_details = self.account_details()
        print(account_details)
        created_at = account_details[0]["created_at"]
        follower_count = int(account_details[0]["followers_count"])
        following_count = int(account_details[0]["following_count"])
        follower_to_following_ratio = str(follower_count / following_count) if following_count != 0 else 0
        list_count = account_details[0]["list_count"]
        likes_count = account_details[0]["likes_count"]
        tweets_count = account_details[0]["tweet_count"]
        summary_date = account_details[0]["summary_date"]
        days_of_analysis = days_between_dates(analysis_date=summary_date, creation_date=created_at)
        likes_average = str(likes_count / days_of_analysis)
        tweet_average = str(tweets_count / days_of_analysis)

        with open(str(self.artifact / (self.user_name + '_account_summary.txt')), 'w') as report:
            report.writelines('===' + self.user_name + "===")
            report.writelines("created at: " + created_at + '\n')
            report.writelines("days on network, given the analysis date: " + str(days_of_analysis) + '\n')
            report.writelines("follower count: " + str(follower_count) + '\n')
            report.writelines("following count: " + str(following_count) + '\n')
            report.writelines("follower to following ratio: " + follower_to_following_ratio + '\n')
            report.writelines("list count: " + str(list_count) + '\n')
            report.writelines("likes count: " + str(likes_count) + '\n')
            report.writelines("tweets count: " + str(tweets_count) + '\n')
            report.writelines("likes average: " + likes_average + '\n')
            report.writelines("tweet average: " + tweet_average + '\n')
            report.writelines("=== activity ===" + '\n')
            report.writelines([str(each) + '\n' for each in account_activity_documents])
            report.writelines("=== engagement ===" + '\n')
            report.writelines([str(each) + '\n' for each in account_engagement_documents])
            report.writelines("=== most replies ===" + '\n')
            report.writelines([str(each) + '\n' for each in most_replies_to])
            report.writelines("=== most quotes ===" + '\n')
            report.writelines([str(each) + '\n' for each in most_quotes])
            report.writelines("=== most retweets ===" + '\n')
            report.writelines([str(each) + '\n' for each in most_retweets])
            report.writelines("=== retweets since 100 days ago ===" + '\n')
            report.writelines(str(number_of_retweets) + '\n')
            report.writelines("=== tweets since 100 days ago ===" + '\n')
            report.writelines(str(number_of_tweets) + '\n')
            report.writelines("=== replies since 100 days ago ===" + '\n')
            report.writelines(str(number_of_replies) + '\n')
            report.writelines("=== quotes since 100 days ago ===" + '\n')
            report.writelines(str(number_of_quotes) + '\n')
            report.writelines("=== total engagement since 100 days ago ===" + '\n')
            report.writelines(str(total_engagement) + '\n')


'''
Before running this script, run streaming account tweets. 
'''

if __name__ == '__main__':
    pwd = Path(__file__).parent.parent / 'artifacts'
    account_behaviour = AccountAnalysis(_db_name='twitter', _db_url='localhost:27017', _collection='YaarDabestaani_account_tweets',
                                        _user_name='YaarDabestaani', artifacts=pwd)
    account_behaviour.account_summary()
