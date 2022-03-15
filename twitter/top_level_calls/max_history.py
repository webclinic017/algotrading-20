"""Twitter get max history."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.twitter.twitter_api import TwitterAPI
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.twitter.user_tweets.part3_trade_df import CreateTradeRef
    from scripts.dev.twitter.top_level_calls.tweet_timestamps import GetTimestampsForEachRelTweet
    from scripts.dev.multiuse.help_class import help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from twitter.twitter_api import TwitterAPI
    from twitter.methods.helpers import TwitterHelpers
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from twitter.user_tweets.part3_trade_df import CreateTradeRef
    from twitter.top_level_calls.tweet_timestamps import GetTimestampsForEachRelTweet
    from multiuse.help_class import help_print_arg, write_to_parquet

# %% codecell


class TwitterMaxHistory():
    """Get max history for twitter user."""

    def __init__(self, username, **kwargs):
        self._get_class_vars(self, username, **kwargs)
        self.proceed = self._tweet_max_hist(self, **kwargs)
        if self.proceed:
            self._load_clean_hist_tweets(self, **kwargs)
            self._merge_tweet_meta_with_hist(self, **kwargs)
            self.p2 = self._run_p2_trade_extract(self, **kwargs)
            self.p3 = self._run_p3_trade_df(self, **kwargs)

    @classmethod
    def _get_class_vars(cls, self, username, **kwargs):
        """Find class variables, including finding user_id."""
        self.verbose = kwargs.get('verbose', False)
        self.user_id = TwitterHelpers.twitter_lookup_id(username)
        self.username = username

    @classmethod
    def _tweet_max_hist(cls, self, **kwargs):
        """Get twitter max historical tweets for a username."""
        kwargs = ({'username': self.username, 'get_max_hist': True,
                   'params': {'max_results': 100,
                              'exclude': 'retweets,replies'}})
        # First call gets the first round of results - includes pag token
        call = TwitterAPI(method='user_tweets', **kwargs)
        next_token = call.get.json()['meta']['next_token']

        for n in range(31):
            try:
                kwargs['params']['pagination_token'] = next_token
                call = TwitterAPI(method='user_tweets', **kwargs)
                next_token = call.get.json()['meta']['next_token']
            except Exception as e:
                msg1 = "TwitterAPI.tweet_max_hist: "
                msg2 = f"encountered error {str(e)} {type(e)}. Breaking"
                help_print_arg(f"{msg1}{msg2}")
                help_print_arg(f"{str(call.get.json()['meta'])}")
                break

        if n > 2:
            return True
        else:  # For debugging
            self.call = call
            return False

    @classmethod
    def _load_clean_hist_tweets(cls, self, **kwargs):
        """Load and clean historical tweets."""
        df_hist = (TwitterHelpers.tf('user_tweets', username=self.username,
                                     return_df=True)
                                 .drop_duplicates(subset='id')
                                 .reset_index(drop=True))
        # Possible derivatives trades
        df_dervs = (df_hist[
                    ((df_hist['call']) | (df_hist['put']))
                    & (df_hist['sym_0'])])
        # Remove any possible trades that already have timestamps
        if 'created_at' in df_dervs.columns:
            df_dervs = df_dervs[df_dervs['created_at'].isna()]
        df_times_needed = df_dervs[['id', 'author_id']]
        # Get timestamps for any tweets that don't have them
        GetTimestampsForEachRelTweet(user_id=self.user_id, df=df_times_needed, **kwargs)

    @classmethod
    def _merge_tweet_meta_with_hist(cls, self, **kwargs):
        """Merge meta tweets with normal historical tweets."""
        df_tref = (TwitterHelpers.tf('tweet_by_id',
                                     self.user_id, return_df=True))
        df_tref['created_at'] = (pd.to_datetime(df_tref['created_at'],
                                                errors='ignore'))
        df_hist = (TwitterHelpers.tf('user_tweets', username=self.username,
                                     return_df=True))

        cols_to_drop = (df_hist.index.intersection(df_tref.index)
                                     .drop('id', errors='ignore'))
        df_hist = df_hist.loc[:, df_hist.columns.difference(cols_to_drop)]
        df_hist_comb = df_hist.merge(df_tref, on='id', how='left')

        f_hist = TwitterHelpers.tf('user_tweets', username=self.username)
        write_to_parquet(df_hist_comb, f_hist)

    @classmethod
    def _run_p2_trade_extract(cls, self, **kwargs):
        """Run part2_clean_extract info from tweets."""
        return TwitterUserExtract(self.user_id, **kwargs)

    @classmethod
    def _run_p3_trade_df(cls, self, **kwargs):
        """Part 3 - create trade ref class."""
        return CreateTradeRef(self.user_id, **kwargs)
