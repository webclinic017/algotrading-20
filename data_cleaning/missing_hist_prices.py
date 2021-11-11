"""Class for missing historical prices."""
# %% codecell
import os
import sys
from pathlib import Path
from io import BytesIO
import importlib
from tqdm import tqdm

import datetime

import requests
import pandas as pd
import numpy as np


try:
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.api import serverAPI
    # from scripts.dev.data_collect.nasdaq_class import nasdaqShort
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions
    from api import serverAPI

# %% codecell


class MissingHistDates():
    """Process and find missing historical dates."""

    """
    self.df_all : cols 'symbol' and 'date' from 2021 hist
    self.sym_list : list of symbols from ^
    self.date_list : all trading days for this year
    self.all_missing : all stock trading days not in date_list
        & not symbols that started after beginning of year
    self.df_less_than_20 : symbols from ^ with < 20 days missing
    """

    def __init__(self, previous=False, drop_null=True):

        self._df_to_use(self)
        self._dates_to_use(self)

        if isinstance(self.df_all, pd.DataFrame):
            self._get_non_consecutive_dates(self)
            self._less_than_20(self)
            self._write_to_parquet(self)
        else:
            help_print_arg('MissingHistDates: Error getting data from server')

    @classmethod
    def _df_to_use(cls, self):
        """Get and clean dataframe to use."""
        scp_df = serverAPI('stock_close_cb_all').df
        cols_to_use = ['symbol', 'date']
        df = scp_df[cols_to_use].copy()
        df.drop_duplicates(subset=['symbol', 'date'], inplace=True)

        fpath_null = Path(baseDir().path, 'StockEOD/missing_dates/null_dates/_null_dates.parquet')
        if fpath_null.exists():
            df_null = pd.read_parquet(fpath_null)
            df = (pd.merge(df, df_null, on=['date', 'symbol'],
                           how='left', indicator=True)
                    .query('_merge == "left_only"')
                    .drop(columns='_merge', axis=1)
                    .reset_index(drop=True))

        sym_list = df['symbol'].unique().dropna().tolist()

        self.df_all = df.copy()
        self.sym_list = sym_list

    @classmethod
    def _dates_to_use(cls, self):
        """Get date list applicable to iex_previous date."""
        dt = getDate.query('iex_previous')
        date_list = getDate.get_bus_days(this_year=True)
        date_list = date_list[date_list['date'].dt.date <= dt].copy()
        self.date_list = date_list

    @classmethod
    def _get_non_consecutive_dates(cls, self):
        """Get dataframe of all non-consecutive dates."""
        non_consecutives = []
        df = self.df_all

        for sym in tqdm(self.sym_list):
            df_mod = df[(df['symbol'] == sym)]
            # df_dates = date_list[~date_list['date'].isin(df_mod['date'].tolist())].copy()

            date_list_mod = (self.date_list[
                                (self.date_list['date'] >= df_mod['date'].min()) &
                                (self.date_list['date'] <= df_mod['date'].max())
                             ])
            dates_needed = (date_list_mod[
                                ~date_list_mod['date'].isin(df_mod['date'].tolist())
                                ].copy()
                            )

            # df_dates['symbol'] = sym
            dates_needed['symbol'] = sym

            # missing_dfs.append(df_dates)
            non_consecutives.append(dates_needed)

        # Concat all missing data together
        all_missing = pd.concat(non_consecutives)
        self.all_missing = all_missing

    @classmethod
    def _less_than_20(cls, self):
        """Get all symbols with less than 20 dates missing."""
        less_than_20 = (self.all_missing['symbol'].value_counts() < 20).reset_index()
        less_than_20 = (less_than_20[less_than_20['symbol'] == True]
                        .drop(columns=['symbol'])
                        .rename(columns={'index': 'symbol'})
                        )

        df_less_than_20 = (self.all_missing[
                           self.all_missing['symbol'].isin(less_than_20['symbol']
                                                           .tolist())
                           ])
        self.df_less_than_20 = df_less_than_20

    @classmethod
    def _write_to_parquet(cls, self):
        """Write to parquet."""
        dt = getDate.query('iex_previous')
        # Define base path to use for subsequent paths
        bpath = Path(baseDir().path, 'StockEOD/missing_dates')
        # Define date suffix to use
        dsuf = f"_{dt}.parquet"
        # Construct paths
        path = Path(bpath, 'date', dsuf)
        path_less_20 = Path(bpath, 'less_than_20', dsuf)
        path_all = Path(bpath, 'all', dsuf)

        # Write to parquet files
        write_to_parquet(self.df_less_than_20, path_less_20)
        write_to_parquet(self.all_missing, path_all)

# %% codecell
