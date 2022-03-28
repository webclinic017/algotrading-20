"""Get timestamps for each relevant tweet."""
# %% codecell
import time
from inspect import signature

import pandas as pd

try:
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.twitter.watchlists.possible_trades import TwitterPossibleTrades
    from scripts.dev.twitter.twitter_api import TwitterAPI
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.multiuse.df_helpers import DfHelpers
    from scripts.dev.multiuse.help_class import help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from twitter.watchlists.possible_trades import TwitterPossibleTrades
    from twitter.twitter_api import TwitterAPI
    from twitter.methods.helpers import TwitterHelpers
    from multiuse.df_helpers import DfHelpers
    from multiuse.help_class import help_print_arg, write_to_parquet

# %% codecell


class GetTimestampsForEachRelTweet():
    """Get timestamps for any relevant tweets."""

    testing = False

    def __init__(self, user_id, **kwargs):
        self._get_class_vars(self, **kwargs)
        # For get_max_history method, pass in dataframe through kwargs
        self.df = kwargs.get('df', False)
        if not isinstance(self.df, pd.DataFrame):
            self.df = self._get_relevant_tweets(self, user_id)

        if not self.df.empty and not self.testing:
            self._call_tweets_by_id(self, self.df)
            # Write/update local hist with timestamps
            self.df_hist_cb = self._merge_tweet_meta_with_hist(self, user_id)
            print('Historical combined available under: self.df_hist_cb')
            # Need to run TwitterUserExtract again to get the timestamps
            # ^ Double check this
            TwitterUserExtract(user_id)

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Unpack kwargs and get class variables."""
        self.testing = kwargs.get('testing', False)
        self.verbose = kwargs.get('verbose', False)
        # skip_write historical combined dataframe
        self.skip_write = kwargs.get('skip_write', False)

    @classmethod
    def _get_relevant_tweets(cls, self, user_id):
        """Get relevant tweets with user_id."""
        # df_ref = TwitterUserExtract(user_id).df_ref

        # This below contains the tweets with null created_at
        df_ref = TwitterUserExtract(user_id).df
        cols_to_view, df_to_get = ['id', 'author_id'], None

        # Only get timestamps with created_at ~ NaT. New tweets
        if 'created_at' in df_ref.columns:
            df_to_get = df_ref[df_ref['created_at'].isna()][cols_to_view]
        else:  # If created_at doesn't exist, get all timestamps
            df_to_get = df_ref[cols_to_view]

        # Get all watchlist tweet ids needing reference ids
        df_atrades = TwitterPossibleTrades().df_atrades.copy()
        cols_watch = ['id_watch', 'created_at_watch', 'author_id']
        df_watch_to_get = df_atrades[df_atrades['author_id'] == str(user_id)]
        if not df_watch_to_get.empty:
            df_watch_to_get = (df_watch_to_get[
                               df_watch_to_get['created_at_watch'].isna()]
                               .loc[:, cols_watch].drop_duplicates()
                               .drop(columns='created_at_watch')
                               .rename(columns={'id_watch': 'id'}))

        if not df_watch_to_get.empty:
            df_to_get = pd.concat([df_to_get, df_watch_to_get])

        return df_to_get

    @classmethod
    def _call_tweets_by_id(cls, self, df):
        """Iterate through tweets by ID. Store data locally."""
        kwargs = {}
        kwargs['params'] = {}
        payload = ['created_at,author_id,conversation_id,entities']
        method = 'tweet_by_id'

        rows, sleep_num = df.shape[0], 1
        if rows < 100:
            sleep_num = .01
        elif rows >= 150:
            help_print_arg(f"GetTimestampsForEachRelTweet: {str(rows)} rows")

        for index, row in df.iterrows():
            if self.verbose:
                help_print_arg(f"GetTimestampsForEachRelTweet: {row['id']}")
            kwargs['tweet_id'] = row['id']
            kwargs['author_id'] = row['author_id']
            kwargs['params']['tweet.fields'] = payload
            try:  # If there's an error, break the loop - this only get tstamps
                ta = TwitterAPI(method=method, **kwargs)
                time.sleep(sleep_num)
            except Exception as e:
                msg = (f"GetTimestampsForEachRelTweet Error: {type(e)}"
                       f" {str(e)} Breaking for loop")
                help_print_arg(msg)
                break

    @classmethod
    def _merge_tweet_meta_with_hist(cls, self, user_id):
        """Merge meta tweets with normal historical tweets."""
        df_tref = (TwitterHelpers.tf('tweet_by_id',
                                     user_id, return_df=True))
        df_tref['created_at'] = (pd.to_datetime(
                                 df_tref['created_at'],
                                 errors='ignore'))
        df_hist = (DfHelpers.combine_duplicate_columns(
                  (TwitterHelpers.tf(
                   'user_tweets',
                   user_id=user_id,
                   return_df=True))))

        hcols, rcols = df_hist.columns, df_tref.columns
        cols_to_drop = (hcols.intersection(rcols)
                             .drop(['id', 'text'], errors='ignore'))

        df_hist = df_hist.loc[:, hcols.difference(cols_to_drop)]
        df_hist_comb = (df_hist.merge(df_tref.drop(columns='text'),
                                      on='id', how='left')
                               .drop_duplicates(subset='id')
                               .reset_index(drop=True))

        f_hist = TwitterHelpers.tf('user_tweets', user_id=user_id)
        if not self.skip_write:
            write_to_parquet(df_hist_comb, f_hist)

        return df_hist_comb

# %% codecell
