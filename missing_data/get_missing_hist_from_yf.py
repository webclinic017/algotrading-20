"""Get missing historical data using yfinance."""
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
import yfinance as yf


try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.missing_data.missing_hist_prices import MissingHistDates
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.path_helpers import get_most_recent_fpath
    from missing_data.missing_hist_prices import MissingHistDates
    from api import serverAPI

# %% codecell


def get_yf_loop_missing_hist(key='less_than_20', cs=False, sym_list=None, refresh_missing_dates=True):
    """Get less_than_20 syms and call GetYfMissingDates."""

    if sym_list:
        pass
    elif cs is False:
        if refresh_missing_dates:
            MissingHistDates()
        bpath = Path(baseDir().path, f"StockEOD/missing_dates/{key}")
        fpath = get_most_recent_fpath(bpath)
        df_dates = pd.read_parquet(fpath)
        sym_list = df_dates['symbol'].unique().tolist()
    else:
        if refresh_missing_dates:
            MissingHistDates(cs=True)
        bpath = Path(baseDir().path, "StockEOD/missing_dates/all")
        fpath = get_most_recent_fpath(bpath)
        df_dates = pd.read_parquet(fpath)
        # Get all symbols, reduce to common stock and adr's
        sym_list = df_dates['symbol'].unique().tolist()

    for sym in tqdm(sym_list):
        try:
            GetYfMissingDates(sym=sym)
        except Exception as e:
            help_print_arg(f"get_yf_loop_missing_hist error: {str(e)}")

# %% codecell


class GetYfMissingDates():
    """Fill in missing hist data from iex, from yfinance."""

    """
    sym : symbol
    period : default ytd
    interval : default 1d

    self.sym_path : fpath to read/write
    self.sym_df : local dataframe of hist prices
    self.df_yf : yfinance historical prices dataframe
    self.df : Final df to write
    """
    sym_df = None

    def __init__(self, sym, period="ytd", interval="1d", proxy=None):
        self._read_local_df(self, sym)
        self._get_clean_data(self, sym, period, interval, proxy)
        if isinstance(self.sym_df, pd.DataFrame):
            self._merge_concat_dfs(self, self.df_yf, self.sym_df)
        else:
            self.df = self.df_yf
        self._write_to_parquet(self)

    @classmethod
    def _read_local_df(cls, self, sym):
        """Read local df, merge, concat, and overwrite."""
        dt = getDate.query('iex_eod')
        bpath = Path(baseDir().path, f"StockEOD/{str(dt.year)}")
        sym_path = Path(bpath, f"{sym.lower()[0]}/_{sym}.parquet")
        # If sym path is already a local file
        if sym_path.exists():
            sym_df = pd.read_parquet(sym_path)
            # If date is object, convert to datetime64
            if sym_df['date'].dtype == 'O':
                sym_df['date'] = pd.to_datetime(sym_df['date'])
            self.sym_df = sym_df

        self.sym_path = sym_path

    @classmethod
    def _get_clean_data(cls, self, sym, period, interval, proxy):
        """Request and clean data from yfinance."""
        data = yf.download(
                tickers=sym,
                period=period,
                interval=interval,
                group_by='ticker',
                auto_adjust=True,
                prepost=False,
                proxy=proxy
            )

        df = data.reset_index()
        df.insert(1, 'symbol', sym)
        (df.rename(columns={'Date': 'date', 'Open': 'fOpen', 'High': 'fHigh',
                            'Low': 'fLow', 'Close': 'fClose',
                            'Volume': 'fVolume'}, inplace=True))
        df = dataTypes(df, parquet=True).df
        dt = getDate.query('iex_eod')

        self.df_yf = df[df['date'].dt.date <= dt]

    @classmethod
    def _merge_concat_dfs(cls, self, df_yf, sym_df):
        """Merge, concat, and overwrite local df."""
        # Get dates that are present in yahoo data but not iex
        merged_df = pd.merge(df_yf, sym_df, how='left', on=['date', 'symbol'], indicator=True)
        yf_dt_list = merged_df[merged_df['_merge'] == 'left_only']['date'].tolist()
        # Boolean index on missing dates
        yf_dates = df_yf[df_yf['date'].isin(yf_dt_list)]
        # Concat missing dates with iex data
        concat_df = (pd.concat([sym_df, yf_dates])
                       .reset_index(drop=True)
                       .sort_values(by=['date'], ascending=True))
        # Assign cleaned dataframe to self.df
        self.df = concat_df

    @classmethod
    def _write_to_parquet(cls, self):
        """Write to local parquet file."""
        write_to_parquet(self.df, self.sym_path)



# %% codecell
