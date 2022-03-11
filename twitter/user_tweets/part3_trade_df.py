"""Part 3 of User Tweets: Construct Trade DF."""
# %% codecell

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from twitter.methods.helpers import TwitterHelpers

# %% codecell


class CreateTradeRef(TwitterHelpers):
    """Create Trade Reference DF."""

    def __init__(self, user_id, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        df, fpath = self._get_df(self, user_id, **kwargs)
        df_tcode = self._process_for_tcode(self, df)
        self.df_starts = self._add_trade_starts(self, df_tcode)
        self.df_trades = self._make_trade_df(self, self.df_starts, user_id)
        self._write_to_file(self, self.df_trades, fpath)

    @classmethod
    def _get_df(cls, self, user_id, **kwargs):
        """Get dataframe."""
        df = kwargs.get('df', None)
        if not isinstance(df, pd.DataFrame):
            tue = TwitterUserExtract(user_id)
            df = tue.df_view.drop_duplicates(subset=['id']).copy()
        # Get local file path for user trades
        fpath = self.tf('user_trades', user_id=user_id)
        if self.verbose:
            help_print_arg(f"CreateTradeRef: {str(fpath)}")

        return df, fpath

    @classmethod
    def _process_for_tcode(cls, self, df, **kwargs):
        """Process dataframe for tcode (trade code)."""
        # Only keep the following columns
        cols_to_view = (['text', 'sym_0', 'sym_1', 'entry', 'tcode',
                         'exit', 'strike', 'side', 'created_at', 'id'])
        # Set index to sym_0 (trade symbol), sort by sym_0, datetime created
        df = (df[cols_to_view].set_index('sym_0')
                              .sort_values(by=['sym_0', 'created_at']))
        # Only keep tweets that don't contain multiple symbols,
        # then drop the column for second symbol mentioned
        df = df[df['sym_1'].isna()].drop(columns=['sym_1']).copy()

        return df

    @classmethod
    def _add_trade_starts(cls, self, df, **kwargs):
        """Make trade df."""
        df2 = df.reset_index().drop_duplicates(subset=['tcode']).copy()
        df2 = df2[(df2['entry']) | (df2['exit'])].copy()
        df2['trade_start'] = (np.where(
            (df2['entry']), True, False
        ))

        df3 = df.reset_index().copy()
        # I can set the trade start equal to that tweet id
        df3 = df3.join(df2['trade_start']).copy()
        df3['trade_start'] = df3['trade_start'].replace(np.NaN, False)

        return df3

    @classmethod
    def _make_trade_df(cls, self, df, user_id, **kwargs):
        """Make trade df."""
        # Get subset for all rows where entry is true
        cols_to_keep = ['tcode', 'created_at', 'sym_0', 'id']
        df_entries = (df[df['trade_start']][cols_to_keep]
                      .copy()
                      .rename(columns={'created_at': 'entered_at',
                                       'id': 'entry_id'})
                      .set_index('tcode'))
        # Get subset for all rows where exit is true
        cols_to_keep = ['tcode', 'created_at', 'id', 'text']
        df_exits = (df[df['exit']][cols_to_keep]
                    .copy()
                    .drop_duplicates(subset='tcode')
                    .rename(columns={'created_at': 'exit_at',
                                     'id': 'exit_id'})
                    .set_index('tcode'))

        # Extract alll text that has at least 1 number, followed by % sign
        pat = '([0-9]{1,3}%)'
        min_return = (df_exits['text'].str.extract(pat)
                      .rename(columns={0: 'min_return'}))
        df_exits.drop(columns='text', inplace=True)
        # Combine entries, exits, and minimum expected return from that trade
        df_trades = df_entries.join(df_exits).join(min_return)
        df_trades['author_id'] = user_id

        return df_trades

    @classmethod
    def _write_to_file(cls, self, df, fpath, **kwargs):
        """Write to file."""
        skip_write = kwargs.get('skip_write', False)
        if not skip_write:  # Drop on index
            write_to_parquet(df, fpath, combine=True, drop_duplicates=True)

# %% codecell
