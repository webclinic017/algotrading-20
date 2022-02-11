"""Twitter user tweets."""
# %% codecell

import numpy as np
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import write_to_parquet, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import write_to_parquet, help_print_arg

# %% codecell


class TwitterUserTweets():
    """Extract relevant info from Twitter Users' Tweets."""

    def __init__(self, rep, method, fpath):
        df = self._convert_data(self, rep)
        df = self._add_rt_info(self, df)
        df = self._add_calls_puts(self, df)
        df = self._start_creating_cols(self, df)
        df = self._clean_strike_prices(self, df)
        self.df = self._drop_and_write(self, df, fpath)

    @classmethod
    def _convert_data(cls, self, rep):
        """Convert json to dataframe."""
        df = pd.DataFrame(rep.json()['data'])
        return df

    @classmethod
    def _add_rt_info(cls, self, df):
        """Add retweet information from text."""
        df['RT'] = df['text'].str.contains('RT ')
        df['RT_From'] = (np.where(
            df['RT'],
            df['text'].str.split('@').str[1].str.split(':').str[0],
            np.NaN
        ))
        return df.copy()

    @classmethod
    def _add_calls_puts(cls, self, df):
        """Add call/put columns."""
        tlower = df['text'].str.lower().str.contains
        df['call'] = tlower('call')
        df['put'] = tlower('put')
        return df.copy()

    @classmethod
    def _start_creating_cols(cls, self, df):
        """Start converting columns with special characters specified."""
        # Symbol
        cdict = {'regex': '[^\b$A-Za-z\b]+', 'spc_char': '$', 'col_pre': 'sym_'}
        df = self._convert_symbols_into_cols(self, df, cdict)

        cdict = {'regex': '[^\b#a-zA-Z\b]+', 'spc_char': '#', 'col_pre': 'hash_'}
        df = self._convert_symbols_into_cols(self, df, cdict)
        # Strike price
        cdict = {'regex': '[^\b0-9.%\b]+', 'spc_char': '$', 'col_pre': 'val_'}
        df = self._convert_symbols_into_cols(self, df, cdict)

        return df

    @classmethod
    def _convert_symbols_into_cols(cls, self, df, cdict):
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
        except ValueError:
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
                df[col] = df[col].str.replace(cdict['spc_char'], '')
                df[col] = df[col].str.replace('', np.NaN)
            except TypeError:
                df[col] = df[col].replace(cdict['spc_char'], '')
                df[col] = df[col].replace('', np.NaN)
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
                msg1 = 'TwitterUserTweets._clean_strike_prices'
                msg2 = f" {type(ve)} {str(ve)}"
                help_print_arg(f"{msg1}{msg2}")
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
        df.drop_duplicates(subset=['id'], inplace=True)
        write_to_parquet(df, fpath, combine=True)

        return df

# %% codecell
