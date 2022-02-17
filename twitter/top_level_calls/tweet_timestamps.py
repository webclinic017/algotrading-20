"""Get timestamps for each relevant tweet."""
# %% codecell
import time
from inspect import signature

try:
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.twitter.twitter_api import TwitterAPI
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from twitter.twitter_api import TwitterAPI
    from multiuse.help_class import help_print_arg

# %% codecell


class GetTimestampsForEachRelTweet():
    """Get timestamps for any relevant tweets."""

    # .df = df_to_get
    def __init__(self, user_id, **kwargs):
        testing = kwargs.get('testing', False)
        self.df = self._get_relevant_tweets(self, user_id)

        if not self.df.empty and not testing:
            self._call_tweets_by_id(self, self.df)

    @classmethod
    def _get_relevant_tweets(cls, self, user_id):
        """Get relevant tweets with user_id."""
        df_view = TwitterUserExtract(user_id).df_view

        cols_to_view, df_to_get = ['id', 'author_id'], None
        if 'created_at' in df_view.columns:
            df_to_get = df_view[df_view['created_at'].isna()][cols_to_view]
        else:
            df_to_get = df_view[cols_to_view]

        return df_to_get

    @classmethod
    def _call_tweets_by_id(cls, self, df):
        """Iterate through tweets by ID. Store data locally."""
        kwargs = {}
        kwargs['params'] = {}
        payload = ['created_at,author_id,conversation_id,entities']
        method = 'tweet_by_id'

        for index, row in df.iterrows():
            kwargs['tweet_id'] = row['id']
            kwargs['author_id'] = row['author_id']
            kwargs['params']['tweet.fields'] = payload
            try:  # If there's an error, break the loop
                ta = TwitterAPI(method=method, **kwargs)
                time.sleep(1)
            except Exception as e:
                msg = (f"GetTimestampsForEachRelTweet Error: {type(e)}"
                       f" {str(e)} Breaking for loop")
                help_print_arg(msg)
                break


# %% codecell



# %% codecell
