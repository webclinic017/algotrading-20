"""Get all missing/non-null hist prices."""
# %% codecell
import os
from pathlib import Path
from tqdm import tqdm

import pandas as pd
import numpy as np


try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.path_helpers import get_most_recent_fpath
    from api import serverAPI

# %% codecell


class GetMissingDates():
    """Get missing dates from iex."""

    """
    key: 'previous', 'all', 'less_than_20'. Corresponds to fpath dirs
    self.proceed : continue
    self.null_dates : list of null/empty dates
    self.missing_df : dataframe of missing dates
    self.single_df : dataframe of symbols with only one missing date
    self.multiple_df : dataframe of symbols with > 1 missingdate
    """


    def __init__(self, key='less_than_20', proceed=True):
        self.proceed = proceed
        while self.proceed:
            self._get_missing_dates_df(self, key)
            self._get_single_dates(self)
            try:
                self._get_single_dates(self)
                self._get_multiple_dates(self, self.multiple_df)
                self._write_null_dates(self, self.null_dates)
            except Exception as e:
                help_print_error(e, parent='GetMissingDates')
                self._write_null_dates(self, self.null_dates)

    @classmethod
    def _get_missing_dates_df(cls, self, key):
        """Get missing dates."""
        key_options = ['previous', 'all', 'less_than_20']
        if str(key) not in key_options:
            self.proceed = False  # If provided key not in options

        bpath = Path(baseDir().path, f"StockEOD/missing_dates/{key}")
        path = get_most_recent_fpath(bpath)
        df = pd.read_parquet(path)

        # Define path of null dates
        null_path = Path(baseDir().path, 'StockEOD/missing_dates/null_dates', '_null_dates.parquet')
        # Get all data that isn't null/empty
        if null_path.exists():
            null_df = pd.read_parquet(null_path)
            df = (pd.merge(df, null_df, how='left', indicator=True)
                    .query('_merge == "left_only"')
                    .drop(columns=['_merge'], axis=1)
                    .copy())

        self.null_dates = []
        self.missing_df = self._clean_process_missing(self, df)
        self.single_df, self.multiple_df = self._get_single_multiple_dfs(self, self.missing_df)

    @classmethod
    def _clean_process_missing(cls, self, df):
        """Clean and process missing dates df for data requests."""
        # Format dates for iex
        dts = df['date'].drop_duplicates()
        m = pd.Series(dts.dt.strftime('%Y%m%d'))
        m.index = dts
        df['dt'] = df['date'].map(m)
        # Iex exact date url construction
        df['sym_lower'] = df['symbol'].str.lower()
        df['url'] = (df.apply(lambda row:
                              f"/stock/{row['sym_lower']}/chart/date/{row['dt']}?chartByDay=true",
                              axis=1))
        # Construct .parquet paths for reading/writing to local data file
        dt = getDate.query('iex_previous')
        bpath = Path(baseDir().path, 'StockEOD', str(dt.year))
        df['path_parq'] = (df.apply(lambda row:
                                    Path(bpath, row['symbol'].lower()[0], f"_{row['symbol']}.parquet"),
                                    axis=1))
        return df

    @classmethod
    def _get_single_multiple_dfs(cls, self, df):
        """Get single date, multiple date dataframes."""
        vc = df['symbol'].value_counts()
        one_count = vc[vc == 1].index

        df_multiple = df[~df['symbol'].isin(one_count)].copy()
        df_single = df[df['symbol'].isin(one_count)].copy()

        return df_single, df_multiple

    @classmethod
    def _get_single_dates(cls, self):
        """Get all single dates."""
        for index, row in tqdm(self.single_df.iterrows()):
            ud = urlData(row['url'])  # Get exact date data from iex
            if ud.df.empty:  # If dataframe is empty, add to null_dates
                self.null_dates.append(row)
            else:  # Otherwise concat locally/write
                df_all = None
                if Path(row['path_parq']).exists():
                    df_old = pd.read_parquet(row['path_parq'])
                    df_all = pd.concat([df_old, ud.df]).reset_index(drop=True)
                else:
                    df_all = ud.df.copy()

                write_to_parquet(df_all, row['path_parq'])

    @classmethod
    def _get_multiple_dates(cls, self, df):
        """Get all exact date data for multiple dates."""
        symbol_list = df['symbol'].unique()

        for sym in tqdm(symbol_list):
            try:
                # Get a subset of the dataframe
                df_mod = df[df['symbol'] == sym]
                self._multiple_date_loop(self, sym, df_mod)
            except Exception as e:
                help_print_error(e, parent='GetMissingDates')
                self.null_dates.append(df_mod.to_records())

    @classmethod
    def _multiple_date_loop(cls, self, sym, df):
        """Loop for multiple dates. Error handling."""
        # Get a subset of the dataframe
        df_mod = df[df['symbol'] == sym]
        # Set empty df list
        df_list = []
        # Iterate through subset
        for index, row in df_mod.iterrows():
            ud = urlData(row['url'])
            if ud.df.empty:
                self.null_dates.append(row)
            else:
                df_list.append(ud.df)

        if df_list:  # If length > 0
            df_all = pd.concat(df_list)

            if Path(row['path_parq']).exists():
                df_old = pd.read_parquet(row['path_parq'])
                df_all = pd.concat([df_old, df_all]).reset_index(drop=True)
            else:
                df_all = ud.df.copy()

            write_to_parquet(df_all, row['path_parq'])

    @classmethod
    def _write_null_dates(cls, self, null_dates):
        """Write null/empty dates to local df."""
        null_all = pd.DataFrame.from_records(null_dates)

        null_cols_keep = ['date', 'symbol']
        null_keep = null_all[null_cols_keep].reset_index(drop=True)
        null_path = Path(baseDir().path, 'StockEOD/missing_dates/null_dates', '_null_dates.parquet')
        if null_path.exists():
            null_keep = pd.concat([pd.read_parquet(null_path), null_keep]).reset_index(drop=True)

        null_keep.to_parquet(null_path)

# %% codecell
