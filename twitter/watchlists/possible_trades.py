"""Possible trades. Pulling from other classes."""
# %% codecell

import pandas as pd
import numpy as np

try:
    from scripts.dev.twitter.watchlists.watchlist_methods.clean_tweetref import TwitterTweetRefCleanParse
    from scripts.dev.twitter.watchlists.watchlist_methods.find_watchlists import TwitterWatchlists
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
except ModuleNotFoundError:
    from twitter.watchlists.watchlist_methods.clean_tweetref import TwitterTweetRefCleanParse
    from twitter.watchlists.watchlist_methods.find_watchlists import TwitterWatchlists
    from twitter.methods.helpers import TwitterHelpers

# %% codecell


class TwitterPossibleTrades(TwitterWatchlists, TwitterTweetRefCleanParse):
    """From watchlists to possible trades."""

    def __init__(self, **kwargs):
        self.df_tweets = self._get_tweet_ref(self, **kwargs)
        self.df_pos_trades = self._wlist_to_pos_trades(self, **kwargs)
        self._convert_datetimes(self, **kwargs)
        self.df_atrades = self._merge_and_clean(self, **kwargs)
        if self.verbose:
            print('TwitterPossibleTrades: Final dataframe self.df_atrades')

    @classmethod
    def _get_tweet_ref(cls, self, **kwargs):
        """Get cleaned tweet ref df."""
        # Instantiate twitter ref clean_parse
        TwitterTweetRefCleanParse.__init__(self, **kwargs)
        df_tweets = self.df_ctweetref.copy()
        return df_tweets

    @classmethod
    def _wlist_to_pos_trades(cls, self, **kwargs):
        """Create possible trade list from watchlist."""
        # Instantiate twitter watchlists class
        TwitterWatchlists.__init__(self, **kwargs)
        df_allW = self.df_allW.copy()
        # Combine final df with original df for text, other data
        df_all = self.df_tw[self.df_tw.index.isin(df_allW['ogIdx'])].copy()
        cols = ['id', 'author_id', 'created_at', 'text']
        df_all_w = (df_allW.merge(df_all[cols], left_on='ogIdx',
                                  right_index=True, how='left')
                           .dropna(subset='author_id'))
        return df_all_w

    @classmethod
    def _convert_datetimes(cls, self, **kwargs):
        """Convert datetime to EST/ExpDate correct formatting."""
        th_func = TwitterHelpers.convert_ca_tz_exp_date
        self.df_tweets = th_func(self.df_tweets)
        self.df_pos_trades = th_func(self.df_pos_trades)

    @classmethod
    def _merge_and_clean(cls, self, **kwargs):
        """Merge and and clean dataframes. Create all trade refs."""
        # Df all trades with ref data
        df_atrades = (self.df_pos_trades.merge(self.df_tweets,
                      on=['symbol', 'next_exp', 'author_id'], how='left'))
        # Rename columns for respective dataframes
        df_atrades.columns = (df_atrades.columns
                              .str.replace('_x', '_watch')
                              .str.replace('_y', '_ref'))
        # Create a column for tweets during market hours
        og_dti = pd.DatetimeIndex(df_atrades['created_at_ref']).to_frame()
        dti_btwn = og_dti.between_time('9:20', '16:00')
        df_atrades['mkt_hours'] = np.NaN
        mkt_hours_open = (df_atrades[df_atrades['created_at_ref']
                          .isin(dti_btwn.index)].index)
        df_atrades.loc[mkt_hours_open, 'mkt_hours'] = True
        # Create a column from watchlist tweet id with symbol
        df_atrades['id_watch_sym'] = (df_atrades['id_watch'].astype('str')
                                      + '_' +
                                      df_atrades['symbol'].astype('str'))

        return df_atrades
