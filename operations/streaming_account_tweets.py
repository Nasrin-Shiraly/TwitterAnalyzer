from pathlib import Path
import tweepy

from db_collection_structures.interactions import Interactions
from utils.db_utilities.mongodb_setup import DBConnection
from utils.twitter_services.authentication import TwitterConnection
from utils.utilities import formatted_created_at

'''
Input:
_credential_file_path -> str ; Twitter API credentials
db_alias and db_url -> str
collection -> str; name of the collection where tweets are kept
number_of_tweets -> int; the number of tweets to be analyzed
user_name -> str; twitter handle of the account to be analyzed

Output:
A DB collection with the given name.

Result:
an account's interactions of the given number of tweets. To show the network, and its activity.

'''


class AccountInteractions:
    def __init__(self, _credential_file_path, db_alias, db_url, collection, number_of_tweets, user_name):
        self.user_name = user_name

        self.conn = DBConnection(db_alias, db_url)
        self.db = self.conn.db_initiation()

        self.twitter_handle = TwitterConnection(credential_file_path)
        self.api = self.twitter_handle._api

        self.db_name = db_alias
        self.db_url = db_url
        self.collection = collection

        self.number_of_tweets = number_of_tweets

    def fetch_information(self):
        statuses = []

        for pos, status in enumerate(tweepy.Cursor(self.api.user_timeline, id=self.user_name, include_rts=True).items(
                self.number_of_tweets)):
            statuses.append(status)
        interaction = Interactions(collection=self.collection, db=self.db_name, db_url=self.db_url)
        for pos, item in enumerate(statuses):

            account = item.user.screen_name.lower().strip()
            created_at = formatted_created_at(item._json["created_at"])
            print(item)
            try:
                retweeted_account = item.retweeted_status.user.screen_name
                interaction.document_insert(interaction_type="retweet", interaction_owner=account,
                                            interaction_towards=retweeted_account.lower().strip(),
                                            interaction_date=created_at,
                                            interaction_object=item._json)
            except (KeyError, AttributeError):
                try:
                    quoted_account = item.quoted_status.user.screen_name
                    interaction.document_insert(interaction_owner=account,
                                                interaction_towards=quoted_account.lower().strip(),
                                                interaction_date=created_at,
                                                interaction_object=item._json, interaction_type='quote')
                except (KeyError, AttributeError):
                    if item.in_reply_to_screen_name:
                        replied_account = item.in_reply_to_screen_name
                        interaction.document_insert(interaction_owner=account,
                                                    interaction_towards=replied_account.lower().strip(),
                                                    interaction_date=created_at,
                                                    interaction_object=item._json, interaction_type='reply')
                    else:
                        interaction.document_insert(interaction_owner=account,
                                                    interaction_towards=account,
                                                    interaction_date=created_at,
                                                    interaction_object=item._json, interaction_type='tweet')

        if statuses:
            del statuses

    def find_account_by_tweet_id(self, quoted_tweet_id: int):
        tweet = self.api.get_status(quoted_tweet_id)
        return tweet.user.screen_name


'''
Run keyword analysis and pass in high risk accounts here
'''

if __name__ == '__main__':
    path = Path(__file__).parent.parent
    credential_file_path = path / 'credentials' / 'credentials.json'
    user = 'YaarDabestaani'

    operation = AccountInteractions(_credential_file_path=credential_file_path, db_alias='twitter',
                                    db_url='localhost:27017', number_of_tweets=2000, user_name=user,
                                    collection='YaarDabestaani_account_tweets')
    operation.fetch_information()
