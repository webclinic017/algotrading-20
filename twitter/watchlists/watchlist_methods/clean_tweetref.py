"""Clean tweetref class."""
# %% codecell

import pandas as pd
import numpy as np


try:
    from scripts.dev.api import serverAPI
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
except ModuleNotFoundError:
    from api import serverAPI
    from twitter.methods.helpers import TwitterHelpers

# %% codecell


class TwitterTweetRefCleanParse():
    """Clean and parse twitter tweetfref."""

    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.df_twit_tsignals = self._get_df_return_copy(self)
        self.df_ref_clean = (self._tweetref_initial_clean(self,
                             self.df_twit_tsignals, **kwargs))
        # self.df = self._parse_tcode(self, self.df_iclean, **kwargs)
        self.df_ctweetref = TwitterHelpers.parse_tcode(self.df_ref_clean)

        if self.verbose:
            print('Final df accessible under self.df_ctweetref')

    @classmethod
    def _get_df_return_copy(cls, self, **kwargs):
        """Get dataframe and return copy."""
        df_tweet_ref = serverAPI('twitter_trade_signals').df
        df_tweet_ref = (df_tweet_ref.sort_values(by='created_at', ascending=False)
                                    .reset_index(drop=True)
                                    .drop_duplicates(subset='id').copy())
        df = df_tweet_ref.copy()

        return df

    @classmethod
    def _tweetref_initial_clean(cls, self, df, **kwargs):
        """Initial clean of tweetref for analysis."""
        if df['created_at'].dt.tz == 'UTC':
            df['created_at'] = (df['created_at']
                                .dt.tz_convert(None)
                                .dt.tz_localize('UTC')
                                .dt.tz_convert('US/Eastern'))

        tags = (df['entities.hashtags']
                .dropna()
                .apply(lambda x: x[0]['tag']))
        df['tags'] = np.NaN
        df.loc[tags.index, 'tags'] = tags

        cols_to_exclude = (df.columns[df.columns.str
                                        .contains('entities.', regex=True)])

        df = df.drop(columns=cols_to_exclude).copy()
        df.dropna(subset='tcode', inplace=True)

        return df

    @classmethod
    def _parse_tcode(cls, self, df, **kwargs):
        """Parse tcode and return formatted dataframe."""
        df_tcode = df['tcode'].str.split('_', expand=True)
        # Define columns used for the 4 parsed tcode cols
        cols = ['symbol', 'side', 'expDate', 'strike']
        df_tcode.columns = cols
        df_tcode['expDate'] = pd.to_datetime(df_tcode['expDate'], format='%Y%m%d')
        # Merge with original dataframe
        df = df.join(df_tcode).drop(columns='conversation_id')

        return df
