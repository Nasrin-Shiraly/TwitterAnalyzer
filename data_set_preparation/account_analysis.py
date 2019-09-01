import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.dates import HourLocator

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
        interaction_types = '_'.join(each for each in engagement_type)

        pipeline = [{"$match": {"interaction_owner": self.user_name}},
                    {"$match": {"interaction_date": {"$gt": date_of_n_utc_date_ago(days_ago)}}},
                    {"$match": {"interaction_type": {"$in": engagement_type}}},

                    {"$project": {
                        "interaction_date": "$interaction_date",
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

        garnered_engagement_documents = list(self.collection.aggregate(pipeline))

        engagement_pd = pd.DataFrame(columns=['date', 'average_likes', 'average_retweets'])
        for pos, item in enumerate(garnered_engagement_documents):
            engagement_pd.loc[pos, 'date'] = item['_id'].date()
            engagement_pd.loc[pos, 'average_likes'] = item['avg_likes']
            engagement_pd.loc[pos, 'average_retweets'] = item['avg_retweets']

        engagement_pd.sort_values(by=['date'], inplace=True)
        fig, ax = plt.subplots(1, 3, figsize=(15, 15))

        ax[0].plot(engagement_pd['date'], engagement_pd['average_likes'], color='red')
        ax[0].tick_params(axis='x', rotation=45)
        ax[0].xaxis.set_major_locator(plt.MaxNLocator(10))
        ax[0].set_title("Average Likes per Day")
        ax[0].set_ylabel("Average Likes")
        ax[0].set_xlabel("Date")

        ax[1].plot(engagement_pd['date'], engagement_pd['average_retweets'], color='blue')
        ax[1].tick_params(axis='x', rotation=45)
        ax[1].xaxis.set_major_locator(plt.MaxNLocator(10))
        ax[1].set_title("Average Retweets Per Day")
        ax[1].set_ylabel("Average Retweets")
        ax[1].set_xlabel("Date")

        ax[2].plot(engagement_pd['date'], engagement_pd['average_likes'], label='Likes', color="red")
        ax[2].plot(engagement_pd['date'], engagement_pd['average_retweets'], label="Retweets", color="blue")
        ax[2].tick_params(axis='x', rotation=45)
        ax[2].xaxis.set_major_locator(plt.MaxNLocator(10))
        ax[2].set_title("Average Likes and Retweets Per Day")
        ax[2].set_ylabel("Average Likes and Retweets")
        ax[2].set_xlabel("Date")
        plt.subplots_adjust(left=0.06, bottom=0.19, right=0.97, top=0.68)
        ax[2].legend()
        plt.savefig(str(self.artifact / (self.user_name + "_" + interaction_types + '_garnered_interactions.png')),
                    dpi=1000)

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
        pd.plotting.register_matplotlib_converters()
        activity_pattern_pd = pd.DataFrame(columns=['time', 'date'])
        for pos, item in enumerate(account_activity_documents):
            activity_pattern_pd.loc[pos, 'time'] = item['interaction_date'].time()
            activity_pattern_pd.loc[pos, 'date'] = item['interaction_date'].date()
        activity_pattern_pd.sort_values(by=['date'], inplace=True)
        fig, ax = plt.subplots(1, 1, figsize=(15, 15))

        ax.plot_date(activity_pattern_pd['date'], activity_pattern_pd['time'], color='purple',
                     label=' Activity Plot: ' + interaction_types + " / Twitter handle: @" + self.user_name)
        ax.tick_params(axis='x', rotation=45)
        ax.tick_params(axis='y', rotation=45)
        ax.xaxis.set_major_locator(plt.MaxNLocator(10))
        ax.yaxis.set_major_locator(plt.MaxNLocator(24))
        
        loc = HourLocator()

        ax.yaxis.set_major_locator(loc)

        ax.set_title("Activity per Day")
        ax.set_ylabel("Time")
        ax.set_xlabel("Date")
        plt.subplots_adjust(left=0.06, bottom=0.19, right=0.97, top=0.68)
        ax.legend()
        plt.savefig(str(self.artifact / (self.user_name + '_' + interaction_types + '_activity.png')), dpi=1000)

        return account_activity_documents, garnered_engagement_documents

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

    def account_text(self, interaction_type):
        pipeline = [
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
                    "text": "$interaction_object.full_text",
                }
            }
        ]
        _documents = list(self.collection.aggregate(pipeline))
        return _documents

    def find_tweets_with_pattern(self, keywords, interaction_type):

        documents = self.account_text(interaction_type)

        clusters = {key: [] for key in keywords}
        for doc in documents:
            for keyword in keywords:
                if keyword.lower().strip() in doc['text'].lower().strip():
                    clusters[keyword].append(doc)

        with open(str(self.artifact / (self.user_name + 'tweet_pattern_summary.txt')), 'w') as summary:
            summary.writelines('number of documents: ' + str(len(documents)) + '\n')
            for key in clusters.keys():
                summary.writelines(f'{key} has been repeated {len(clusters[key])} times' + '\n')
            summary.writelines(100 * "====" + '\n')

            for each in clusters[key]:
                summary.writelines(each['text'] + '\n')


'''
Before running this script, run streaming account tweets. 
'''

if __name__ == '__main__':
    pwd = Path(__file__).parent.parent / 'artifacts'
    account_behaviour = AccountAnalysis(_db_name='twitter', _db_url='localhost:27017',
                                        _collection='haniehsarkhosh_extended_account_tweets',
                                        _user_name='haniehsarkhosh', artifacts=pwd)
    # account_behaviour.account_summary()
    #
    # account_behaviour.find_tweets_with_pattern(interaction_type=['tweet', 'reply'],
    #                                            keywords=['یار دبستانی', '@yaardabestaani'])
    account_behaviour.display_engagement(['tweet'], 90)
