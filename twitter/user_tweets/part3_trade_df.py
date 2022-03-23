"""Part 3 of User Tweets: Construct Trade DF."""
# %% codecell

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from twitter.methods.helpers import TwitterHelpers
    from api import serverAPI

# %% codecell


class CreateTradeDfV2(TwitterHelpers):
    """Second iteration. Create dataframe of trades to be used for tgram."""

    r_syms = r'\$[A-Z]+'
    r_cp = r' [0-9]+\.?\d{1}([cp]{1}| call | put)'
    r_exp = r'(\d{1,2}[/]\d{1,2}[/]?\d{0,4})'

    def __init__(self, user_id=None, **kwargs):
        self.fpath = self._get_class_vars(self, user_id, **kwargs)
        self.df_m = self._get_merge_dataframes(self, user_id)
        self.df_fc = self._ctd_filter_clean(self, self.df_m)
        # Dataframe pre merge
        self.df_pre = self._df_symbol_call_put(self, self.df_fc)
        # Df exps
        self.df_wExps = self.add_exp_dates(self.df_pre, tcode=True)
        self._entries_non_combined(self, self.df_wExps, self.df_m)

    @classmethod
    def _get_class_vars(cls, self, user_id, **kwargs):
        """Get class variables, unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.skip_write = kwargs.get('skip_write', False)
        self.df = kwargs.get('df', False)

        fpath = self.tf('user_trades', user_id=user_id)
        if not user_id:
            fpath = self.tf('all_trade_entries')
        if self.verbose:
            help_print_arg(f"CreateTradeDfV2 fpath: {str(fpath)}")
        return fpath

    @classmethod
    def _get_merge_dataframes(cls, self, user_id, **kwargs):
        """Get dataframes and if user_id, subset to user."""
        df_hist = serverAPI('twitter_hist_all').df
        df_tweet_ref = serverAPI('twitter_trade_signals').df

        if user_id:
            df_hist = df_hist[df_hist['author_id'] == user_id]
            df_tweet_ref = df_tweet_ref[df_tweet_ref['author_id'] == user_id]
        else:
            print('CreateTradeDfV2: no user_id passed')

        df_prem = (df_hist[pd.Index(['id', 'text'])
                   .append(df_hist.columns
                   .drop(df_tweet_ref.columns, errors='ignore'))])
        df_tref_prem = df_tweet_ref.drop(columns='text')

        df = df_prem.merge(df_tref_prem, on='id', how='left').copy()

        return df

    @classmethod
    def _ctd_filter_clean(cls, self, df):
        """Filter and clean dataframe to one symbol only, non RTs."""
        one_sym = ((df['text'].str.extractall(f"({self.r_syms})")
                              .stack()
                              .reset_index([1, 2], drop=True)
                              .index.value_counts() == 1)
                   .replace(False, np.NaN).dropna())

        cond_no_rt = (~df['RT'])
        one_sym_idx = (df.index.isin(one_sym.index))
        df = df[cond_no_rt & one_sym_idx].copy()

        return df

    @classmethod
    def _df_symbol_call_put(cls, self, df):
        """Extract relevant call/put info."""
        dtsc = df['text'].str.contains
        match_list = []
        match_list.append(dtsc(self.r_syms, case=False))
        match_list.append(dtsc(self.r_cp, case=False))
        matchc = (pd.concat(match_list, axis=1)
                    .all(axis=1)
                    .replace(False, np.NaN)
                    .dropna())

        s_r_exp = (df.loc[matchc.index]['text']
                     .str.extractall(self.r_exp)
                     .reset_index(1, drop=True))
        # Get text with only one expiration date
        s_r_exp = (s_r_exp.loc[s_r_exp.index
                               .drop_duplicates(keep=False)]
                   .copy())

        df_sym_cp = (df.loc[matchc.index]['text'].str
                       .extractall(f"({self.r_syms})({self.r_cp})")
                       .reset_index(1, drop=True)
                       .rename(columns={0: 'symbol', 1: 'strikeCP'})
                       .join(s_r_exp))

        df_st_side = (df_sym_cp['strikeCP'].str
                      .split('(c|p)', expand=True)
                      .drop(columns=[2])
                      .rename(columns={0: 'strike', 1: 'side'}))

        df_trade = (df_sym_cp.join(df_st_side)
                             .drop(columns='strikeCP')
                             .rename(columns={0: 'expDate'})
                             .copy())
        df_trade['symbol'] = (df_trade['symbol'].str
                              .replace('\$', '', regex=True))
        # Merge with original df to get created_at column
        df_trade = df_trade.join(df['created_at']).copy()
        if 2 in df_trade.columns:
            df_trade.drop(columns=[2], inplace=True, errors='ignore')

        # Drop nans for the moment since we need server meta data
        df_trade.dropna(subset='created_at', inplace=True)

        df_pre = df_trade.copy()

        return df_pre

    @classmethod
    def _entries_non_combined(cls, self, df, df_m):
        """Df entries. Non entries, and and the two dataframes combined."""
        df_entries = (df.sort_values(by='created_at')
                        .drop_duplicates(subset='tcode'))
        cols_to_keep = ['id', 'conversation_id', 'author_id', 'text']
        df_entry_full = df_entries.join(df_m[cols_to_keep])
        df_entry_full['expCode'] = (df_entry_full['symbol'].astype('str') + '_'
                                    + df_entry_full['expDate']
                                    .dt.strftime('%Y%m%d'))

        df_nonEntries = (df_m.loc[df_m['created_at'].dropna()
                         .index.drop(df_entries.index)]
                         .copy())
        # From inherited TwitterHelpers function
        df_nonEntExp = self.add_exp_dates(df_nonEntries)

        cols_non_entries = cols_to_keep + ['expDate', 'created_at', 'sym_0']
        df_non_entries = (df_nonEntExp[cols_non_entries]
                          .rename(columns={'sym_0': 'symbol'}))
        df_non_entries['expCode'] = (df_non_entries['symbol'].astype('str') + '_'
                                     + df_non_entries['expDate']
                                     .dt.strftime('%Y%m%d'))

        df_trades = (df_entry_full.merge(
                     df_non_entries, on=['expCode'],
                     how='left', suffixes=('', '_ne')))

        self.df_entries = df_entry_full.copy()
        self.df_non_entries = df_non_entries.copy()
        self.df_trades = df_trades.copy()

    @classmethod
    def _write_to_parquet(cls, self, **kwargs):
        """Write to parquet."""
        kwargs = {'cols_to_drop': ['id']}
        write_to_parquet(self.df_trades, self.fpath, combine=True, **kwargs)

# %% codecell


class CreateTradeRef(TwitterHelpers):
    """Create Trade Reference DF."""

    def __init__(self, user_id, **kwargs):
        self._get_class_vars(self, **kwargs)
        df, fpath = self._get_df(self, user_id, **kwargs)
        df_tcode = self._process_for_tcode(self, df)
        self.df_starts = self._add_trade_starts(self, df_tcode)
        self.df_trades = self._make_trade_df(self, self.df_starts, user_id)
        self._write_to_file(self, self.df_trades, fpath)

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Get class variables, unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.skip_write = kwargs.get('skip_write', False)
        self.df = kwargs.get('df', False)

    @classmethod
    def _get_df(cls, self, user_id, **kwargs):
        """Get dataframe."""
        df = self.df
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
        if not self.skip_write:  # Drop on index
            write_to_parquet(df, fpath, combine=True)

# %% codecell
