"""Twitter user tweets."""
# %% codecell
import warnings

import numpy as np
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import write_to_parquet, help_print_arg
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
except ModuleNotFoundError:
    from multiuse.help_class import write_to_parquet, help_print_arg
    from multiuse.class_methods import ClsHelp
    from twitter.methods.helpers import TwitterHelpers

warnings.simplefilter("ignore")
# %% codecell


class TwitterUserTweets(ClsHelp):
    """Extract relevant info from Twitter Users' Tweets."""
    # Part 1 of user tweets

    def __init__(self, rep, method, fpath, user_id, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        df = self._convert_data(self, rep, user_id)
        # if isinstance(df, pd.DataFrame):
        # new_tweets = self._check_if_new_tweets(self, df, user_id)
        # if new_tweets:
        if isinstance(df, pd.DataFrame):
            df = self._add_rt_info(self, df)
            df = self._add_calls_puts(self, df)
            df = self._start_creating_cols(self, df, **kwargs)
            df = self._clean_strike_prices(self, df)
            self.df = self._drop_and_write(self, df, fpath)

    @classmethod
    def _convert_data(cls, self, rep, user_id):
        """Convert json to dataframe."""
        rjson = rep.json()
        if 'data' in rjson.keys():
            df = pd.DataFrame(rjson['data'])
            df['author_id'] = user_id
            return df
        else:
            # help_print_arg(f"TwitterUserTweets: _convert_data {str(rjson)}")
            return None

    @classmethod
    def _check_if_new_tweets(cls, self, df):
        """Check for any new tweets (intraday polling)."""
        # This should probably just be default.
        fpath_all = TwitterHelpers.tf('all_hist')
        # If combined all file doensn't exist, create it
        if not fpath_all.exists():
            TwitterHelpers.combine_twitter_all()
            # Get all trade signals combined for easy access
            TwitterHelpers.combine_twitter_all(trade_signal=True)

        if not fpath_all.exists():
            return True
        else:
            df_ids = pd.read_parquet(fpath_all, columns=['id'])
            df_new = df[~df['id'].isin(df_ids['id'].tolist())]
            # If no new tweets, don't bother processing
            if df_new.empty:
                return False
            else:
                return True

    @classmethod
    def _add_rt_info(cls, self, df):
        """Add retweet information from text."""
        df['RT'] = df['text'].str.contains('^RT ', regex=True)
        df['RT_From'] = df['text'].str.extract(r'( @[\w]+:)')
        return df.copy()

    @classmethod
    def _add_calls_puts(cls, self, df):
        """Add call/put columns."""
        prem = '( \d+\.?\d?)'
        reg_calls = f'(?={prem}( call|c ))' + '{1}'
        reg_puts = f'(?={prem}( put|p ))' + '{1}'

        tlower = df['text'].str.lower().str.contains
        df['call'] = tlower(reg_calls)
        df['put'] = tlower(reg_puts)
        return df.copy()

    @classmethod
    def _start_creating_cols(cls, self, df, **kwargs):
        """Start converting columns with special characters specified."""
        # Symbol
        cdict = {'regex': '[^\b$A-Za-z\b]+', 'spc_char': '$', 'col_pre': 'sym_'}
        df = self._convert_symbols_into_cols(self, df, cdict, **kwargs)
        # Hashtag
        cdict = {'regex': '[^\b#a-zA-Z\b]+', 'spc_char': '#', 'col_pre': 'hash_'}
        df = self._convert_symbols_into_cols(self, df, cdict, **kwargs)
        # Strike price
        cdict = {'regex': '[^\b0-9.%\b]+', 'spc_char': '$', 'col_pre': 'val_'}
        df = self._convert_symbols_into_cols(self, df, cdict, **kwargs)

        return df

    @classmethod
    def _convert_symbols_into_cols(cls, self, df, cdict, **kwargs):
        """Find all symbols in tweets. Make new cols for each sym."""
        slist = []
        splits = df['text'].str.split(cdict['regex'], regex=True)
        for split in splits:
            split = [sp for sp in split if cdict['spc_char'] in sp]
            if not split:
                split = ''
            slist.append(split)

        try:
            max_len = max([len(sp) for sp in slist if isinstance(sp, list)])
        except ValueError as ve:
            if self.verbose:
                self.elog(self, ve)
            return df

        for n in range(max_len):
            col_list = []
            for s in slist:
                try:
                    col_list.append(s[n])
                except IndexError:
                    col_list.append(0)

            df[f"{cdict['col_pre']}{str(n)}"] = col_list

        df = df.copy()
        new_cols = df.columns[df.columns.str.contains(cdict['col_pre'])]
        for col in new_cols:
            try:  # Get rid of single strings
                df[col] = df[col].str.replace(cdict['spc_char'], '', regex=False)
                df[col] = df[col].str.replace('', np.NaN, regex=False)
            except TypeError:
                df[col] = df[col].replace(cdict['spc_char'], '', regex=False)
                df[col] = df[col].replace('', np.NaN, regex=False)
        # Drop columns where there aren't any non nan values
        df.dropna(axis=1, how='all', inplace=True)

        return df.copy()

    @classmethod
    def _clean_strike_prices(cls, self, df):
        """Clean up possible strike price values."""
        # Sanity check for possible call/put values
        half_vals = list(np.arange(.5, 250, 1))
        int_vals = list(np.arange(1, 50000, 1))
        check_vals = half_vals + int_vals

        cols = df.columns
        val_cols = cols[cols.str.contains('val_')]
        col_excludes = []
        # Convert object cols to floats
        for col in val_cols:
            try:
                df[col] = df[col].astype(np.float64)
            except ValueError as ve:
                if self.verbose:
                    self.elog(self, ve)
                col_excludes.append(col)

        for col in val_cols:
            if col not in col_excludes:
                df[col] = (np.where(
                    df[col].isin(check_vals),
                    df[col], np.NaN
                ))

        df.dropna(axis=1, how='all', inplace=True)

        return df

    @classmethod
    def _drop_and_write(cls, self, df, fpath):
        """Drop duplicates and write to local file."""
        write_to_parquet(df, fpath, combine=True, cols_to_drop=['id'])

        return df

# %% codecell
