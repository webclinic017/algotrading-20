"""Clean and extract information from tweets using regex."""
# %% codecell
from pathlib import Path
import warnings

import pandas as pd
import numpy as np

try:
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.multiuse.df_helpers import DfHelpers
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet, help_print_arg
except ModuleNotFoundError:
    from twitter.methods.helpers import TwitterHelpers
    from multiuse.df_helpers import DfHelpers
    from multiuse.help_class import baseDir, write_to_parquet, help_print_arg


# %% codecell


class TwitterUserExtract():
    """Clean twitter user tweets."""
    # Part 2 of user tweets

    def __init__(self, user_id, dropcols=False, **kwargs):
        df = self._load_filter_tweet_df(self, user_id, **kwargs)
        df_nan = self._remove_cols_with_all_nans(self, df, **kwargs)
        df_call = self._match_put_calls(self, df_nan, call=True)
        df_put = self._match_put_calls(self, df_call, put=True)
        self.df = df_put.copy()
        self.df_view = self._filter_sort_df(self, self.df, dropcols)
        self._update_tweet_by_id_df(self, self.df_view, user_id)

    @classmethod
    def _load_filter_tweet_df(cls, self, user_id, non_rt=False, **kwargs):
        """Load dataframe of tweets for user by username."""
        if 'non_rt' in kwargs.keys():
            non_rt = kwargs['non_rt']

        if user_id is None and 'username' in kwargs.keys():
            username = kwargs['username']
            user_id = TwitterHelpers.twitter_lookup_id(username)

        bpath = Path(baseDir().path, 'social', 'twitter', 'users')
        fpath = bpath.joinpath(str(user_id), '_hist_tweets.parquet')
        df = None
        
        if fpath.exists():
            df = pd.read_parquet(fpath)
            df.drop_duplicates(subset='id', inplace=True)
        else:
            help_print_arg(f"_load_filter_tweet_df: _hist_tweets.parquet does not exist")
        # print(f"Duplicated rows: {str(df['id'].duplicated().sum())}")

        if non_rt:
            df = df[~df['RT']].copy()

        fpath_ref = (TwitterHelpers.twitter_fpaths('tweet_by_id',
                                                   user_id=user_id))
        if fpath_ref.exists():
            df_ref = pd.read_parquet(fpath_ref).drop(columns='text')
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

        if cp_only:
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
    def _match_put_calls(cls, self, df, call=False, put=False):
        """Create columns for put/call match."""
        regf, col = ' (\d\d\d|\d\d|\d)', ''
        if call:
            regf = regf + '(c| call|call)'
            col = 'call'
        elif put:
            regf = regf + '(p| put|put)'
            col = 'put'

        match = df['text'].str.findall(regf)
        df[f"{col}M"] = match[match.map(lambda d: len(d)) > 0]
        df[f"{col}Len"] = df[f"{col}M"].str.len()
        # Unpack [(strike, side)] to just strike, assign to new column
        m1 = match[match.apply(lambda row: len(row) == 1)]
        if 'strike' not in df.columns:
            df['strike'] = np.NaN
            df['side'] = np.NaN
        # Unpack list, assign strike prices to index
        df.loc[m1.index, 'strike'] = m1.apply(lambda row: row[0][0])
        df.loc[m1.index, 'side'] = m1.apply(lambda row: row[0][1])

        # df contains
        dtsc = df['text'].str.contains
        # Add a column for unusual whales
        uw = 'unusual_whales'
        df['uw'] = dtsc(uw, regex=True, case=False)
        # Add a column for watchlist
        uw = 'watch'
        df['watch'] = dtsc(uw, regex=True, case=False)

        return df

    @classmethod
    def _filter_sort_df(cls, self, df, dropcols):
        """Filter df for viewing."""
        df = df.copy()
        cond1 = df['callM'].dropna()
        cond2 = df['putM'].dropna()
        cp_indx = cond1.index.union(cond2.index)

        # Assume that the relevant symbol is stored in col sym_0
        mod2 = df.loc[cp_indx]['sym_0'].dropna()
        df_m3 = df.loc[mod2.index]
        # Default to non-retweet symbols
        df_m3['RT'] = df_m3['RT'].astype('bool')
        df_m4 = df_m3[~df_m3['RT']].copy()

        # Define regex vars
        entry = '(entry|bought)'
        lotto = '(lotto|lotto-|risk)'
        exit = '(up|/%|all out|sell|sold|trim)'

        strc = df_m4['text'].str.contains
        # Disable string extract warning
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_m4['entry'] = strc(entry, regex=True, case=False)
            df_m4['lotto'] = strc(lotto, regex=True, case=False)
            df_m4['exit'] = strc(exit, regex=True, case=False)
            df_m4['uw'] = strc('unusual_whales', regex=True, case=False)

        # callM or putM with more than 2 strike prices are often recaps
        # When there more than 2 for each, it's definitely a recap
        df_m4['recap'] = np.where(
            ((df_m4['callM'].str.len() > 1)
             & (df_m4['putM'].str.len() > 1)),
            True, False
        )

        if dropcols:
            cols_to_view = (['callM', 'putM', 'entry', 'lotto', 'exit',
                             'recap', 'sym_0', 'hash_0', 'text',
                             'id', 'author_id'])
            df_m4 = df_m4[cols_to_view]

        df_m4 = df_m4.copy()

        return df_m4

    @classmethod
    def _update_tweet_by_id_df(cls, self, df, user_id):
        """Update and combine df_view with user_ref_tweets file."""
        # Get relevant trades for the user
        upath = TwitterHelpers.tf('tweet_by_id', user_id)

        if upath.exists():
            df_uref = pd.read_parquet(upath)

            cols_to_keep = ['text', 'author_id', 'id']
            # ids not in utweets
            df_pos = df[cols_to_keep].copy()
            df_new = df_pos[~df_pos['id'].isin(df_uref['id'])].copy()
            # If dataframe is not empty, combine
            if not df_new.empty:
                df_new = pd.concat([df_new, df_uref])
                write_to_parquet(df_new, upath)
        else:
            write_to_parquet(df, upath)


# %% codecell
