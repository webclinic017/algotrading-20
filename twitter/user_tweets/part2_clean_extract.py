"""Clean and extract information from tweets using regex."""
# %% codecell
from pathlib import Path
import warnings
import inspect

import pandas as pd
import numpy as np

try:
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.multiuse.df_helpers import DfHelpers
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet, help_print_arg
except ModuleNotFoundError:
    from twitter.methods.helpers import TwitterHelpers
    from multiuse.df_helpers import DfHelpers
    from multiuse.class_methods import ClsHelp
    from multiuse.help_class import baseDir, write_to_parquet, help_print_arg

# %% codecell


class TwitterUserExtract():
    """Clean twitter user tweets."""
    # Part 2 of user tweets

    prem = '( \d+\.?\d?)'
    reg = f'(?={prem}( call|c ))|(?={prem}( put|p ))'
    reg1 = reg + '{1}'
    entry = '(entry|bought)'
    lotto = '(lotto|lotto-|risk)'
    out = '(up|/%|all out|sell|sold|trim)'

    def __init__(self, user_id, **kwargs):
        self._get_class_vars(self, user_id, **kwargs)
        df = self._load_filter_tweet_df(self, user_id, **kwargs)
        df_nan = self._remove_cols_with_all_nans(self, df, **kwargs)
        self.df_filt = self._match_put_calls(self, df_nan)
        self.df = self._filter_entry_exit_cols(self, self.df_filt, **kwargs)
        self.df_view = self._add_tcode(self, self.df)
        # Updated dataframe for user reference tweets
        self.df_ref = self._update_tweet_ref_with_tcode(self, user_id, self.df_view)

    @classmethod
    def _get_class_vars(cls, self, user_id, **kwargs):
        """Unpack kwargs and assign to class variables."""
        self.f_tweet_by_id = TwitterHelpers.tf('tweet_by_id', user_id)
        # Unpack kwargs
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.skip_write = kwargs.get('skip_write', False)
        self.cp_only = kwargs.get('cp_only', False)

    @classmethod
    def _load_filter_tweet_df(cls, self, user_id, non_rt=False, **kwargs):
        """Load dataframe of tweets for user by username."""
        if user_id is None and 'username' in kwargs.keys():
            username = kwargs['username']
            user_id = TwitterHelpers.twitter_lookup_id(username)

        df = TwitterHelpers.tf('user_tweets', user_id, return_df=True)

        if not isinstance(df, pd.DataFrame):
            msg = "_hist_tweets.parquet does not exist"
            help_print_arg(msg, isp=inspect.stack())

        fpath_ref = (TwitterHelpers.tf('tweet_by_id', user_id=user_id))

        if fpath_ref.exists():
            df_ref = pd.read_parquet(fpath_ref).drop(columns='text')
            if not df_ref.empty:
                df = pd.merge(df, df_ref, on='id', how='left')
                df = DfHelpers.combine_duplicate_columns(df)
                df.reset_index(drop=True, inplace=True)

        return df

    @classmethod
    def _remove_cols_with_all_nans(cls, self, df, cp_only=False, **kwargs):
        """Go through and remove any columns that only contains NaNs."""
        if 'cp_only' in kwargs.keys():
            cp_only = kwargs['cp_only']

        cols = df.columns
        sym_cols = cols[cols.str.contains('sym_')]
        hash_cols = cols[cols.str.contains('hash_')]
        val_cols = cols[cols.str.contains('val_')]
        pos_cols = sym_cols.union(hash_cols).union(val_cols)

        if self.cp_only:
            cond1 = ((df['call']) | (df['put']))
            df = df[cond1].reset_index(drop=True).copy()

        cols_to_drop = []
        for col in pos_cols:
            if df[col].isna().all():
                cols_to_drop.append(col)
                print(f"Dropping col {col} for all NaNs")

        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)

        return df

    @classmethod
    def _match_put_calls(cls, self, df):
        """Create columns for put/call match."""
        df_cp = df[df['text'].str.contains(self.reg)].copy()
        # Remove any rows without sym_0
        idx1 = df_cp['sym_0'].dropna().index
        idx2 = df_cp[df_cp['sym_1'].isna()].index
        idx3 = df_cp[~df_cp['RT']].index
        idx_keep = idx1.intersection(idx2).intersection(idx3)
        # Apply conditions and only keep rows that match
        ex_all = (df_cp.loc[df_cp.index.isin(idx_keep)]
                  ['text'].str.extractall(self.reg)
                  .reset_index(level=1, drop=True))
        # Remove duplicate row nan s
        cols = ['strike', 'side']
        repl_dict = {'side': {'call': 'c', 'put': 'p'}}
        ex1, ex2 = ex_all[[0, 1]], ex_all[[2, 3]]
        ex1.columns, ex2.columns = cols, cols
        # Combine and rejoin into historical df
        df_all = (pd.concat([ex1, ex2])
                    .dropna()
                    .sort_index()
                    .replace(repl_dict, regex=True)
                    .copy())
        df_all['trades'] = True
        df_all['side'] = df_all['side'].str.strip()
        df_all['strike'] = df_all['strike'].str.strip()
        df_filt = df.join(df_all)

        return df_filt

    @classmethod
    def _filter_entry_exit_cols(cls, self, df_filt, **kwargs):
        """Filter df for viewing."""
        dtsc = df_filt['text'].str.contains
        df_filt['watch'] = dtsc('watch', regex=True, case=False)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_filt['entry'] = dtsc(self.entry, regex=True, case=False)
            df_filt['lotto'] = dtsc(self.lotto, regex=True, case=False)
            df_filt['exit'] = dtsc(self.out, regex=True, case=False)
            df_filt['uw'] = dtsc('unusual_whales', regex=True, case=False)

        df_f2 = df_filt[['sym_1', 'sym_2']].dropna()
        df_f2['recap'] = True
        # Combine with df_filt
        df_cleaned = df_filt.join(df_f2['recap']).copy()

        return df_cleaned

    @classmethod
    def _add_tcode(cls, self, df):
        """Construct and add tcode to dataframe."""
        # Find the next expiration date (assume Friday)
        try:
            df['next_exp'] = (df['created_at']
                              + pd.offsets.Week(n=0, weekday=6)
                              - pd.DateOffset(2))
        except TypeError:
            df['next_exp'] = (pd.to_datetime(df['created_at'], errors='ignore')
                              + pd.offsets.Week(n=0, weekday=6)
                              - pd.DateOffset(2))
        df['next_exp'] = pd.to_datetime(df['next_exp'].dt.date)

        # %% codecell
        # Create tcode separated by underscore
        df['tcode'] = (df['sym_0'].astype('str') + '_'
                       + df['side'].astype('str') + '_'
                       + df['next_exp'].dt.strftime('%Y%m%d') + '_'
                       + df['strike'].astype('str'))
        # Filter to only tweets with strike/side values
        df.dropna(subset=['strike', 'side'], inplace=True)
        return df

    @classmethod
    def _update_tweet_ref_with_tcode(cls, self, user_id, df, **kwargs):
        """Update the local user_ref tweets with tcodes."""
        df_v1 = (df.loc[df['trades'].dropna().index]
                   .loc[:, ['id', 'tcode']]
                   .drop_duplicates(subset=['id'])
                   .copy())
        # Read local user_tweet_ref file
        df_tref = (TwitterHelpers.tf('tweet_by_id', user_id, return_df=True)
                                 .drop(columns='tcode', errors='ignore'))
        # Combine dataframes to update tcodes
        df_tref2 = (df_tref.join(df_v1.set_index('id'), on='id', how='left')
                           .reset_index(drop=True)
                           .dropna(subset='text'))
        # Fill NaNs with 0 if column exists
        if 'telegram_sent' in df_tref2.columns:
            df_tref2['telegram_sent'] = df_tref2['telegram_sent'].fillna(0)
        if not self.skip_write:
            write_to_parquet(df_tref2, self.f_tweet_by_id)

        return df_tref2

# %% codecell
