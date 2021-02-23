"""
Implement IEX data requests
"""
# %% codecell
######################################################
import os
import os.path

import json
from io import StringIO, BytesIO

import importlib
import sys
import xml.etree.ElementTree as ET

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

import datetime
from datetime import date, timedelta, time

try:
    from scripts.dev.help_class import baseDir, getDate
except ModuleNotFoundError:
    from help_class import baseDir, getDate

# %% codecell
######################################################

class readData():
    """All purpose class to read local IEX data."""

    @staticmethod
    def all_iex_symbols():
        """Read all IEX symbols."""
        symbols_base = f"{baseDir().path}/tickers"
        all_symbols_fname = f"{symbols_base}/all_symbols.gz"
        ticker_df = pd.read_json(all_symbols_fname, compression='gzip')
        return ticker_df

    @staticmethod
    def etf_list():
        """Read local etf list."""
        etf_fname = f"{baseDir().path}/tickers/etf_list.gz"
        if not os.path.isfile(etf_fname):
            symbols = urlData("/ref-data/symbols").df
            etf_df = pd.DataFrame(symbols[symbols['type'] == 'et']['symbol'])
            etf_df.reset_index(inplace=True, drop=True)
            etf_df.to_json(etf_fname, compression='gzip')
        else:
            etf_df = pd.read_json(etf_fname, compression='gzip')
        return etf_df

    @staticmethod
    def _get_etf_list(etf_fname):
        """Get etf list data from IEX and write to json."""


    @staticmethod
    def last_bus_day_syms():
        """Read all symbols from the last business day."""
        last_date = getDate().query('last_syms')
        syms_fname = f"{baseDir().path}/tickers/new_symbols/{last_date}.gz"

        if not os.path.isfile(syms_fname):
            print('Not data available. Defaulting to mid Feb symbols.')
            syms_fname = f"{baseDir().path}/tickers/all_symbols.gz"
        # Read local json file
        try:
            old_syms = pd.read_json(syms_fname, compression='gzip')
        except ValueError:
            old_syms = pd.DataFrame()
        return old_syms


# %% codecell
######################################################


class expDates():
    """Get option expiration dates."""

    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.dates = self.get_data(self)
        print('Dates accessible in xxx.dates')

    @classmethod
    def get_data(cls, self):
        """Construct get request URL."""
        # Load environment variables
        load_dotenv()
        base_url = os.environ.get("base_url")
        payload = {'token': os.environ.get("iex_publish_api")}
        exp_url = f"{base_url}/stock/{self.symbol}/options"

        exp_dates = self._get_request(self, payload, exp_url)
        return exp_dates

    @classmethod
    def _get_request(cls, self, payload, exp_url):
        """Get expiration data from IEX cloud."""
        exp_bytes = requests.get(exp_url, params=payload)
        exp = json.load(StringIO(exp_bytes.content.decode('utf-8')))

        return exp

# %% codecell
######################################################

class urlData():
    """Get data and convert to pd.DataFrame."""
    # url_suf = url suffix. Should be string with
    # 1st character as a forward slash

    def __init__(self, url_suf):
        print('Data accessible by xxx.df')
        self.df = self._get_data(self, url_suf)

    @classmethod
    def _get_data(cls, self, url_suf):
        """Get data from IEX, return dataframe."""
        load_dotenv()
        base_url = os.environ.get("base_url")
        payload = {'token': os.environ.get("iex_publish_api")}
        get = requests.get(f"{base_url}{url_suf}", params=payload)
        df = pd.read_json(BytesIO(get.content))
        return df


# %% codecell
######################################################


"""
Writing etf_list to local json file

etf_list = pd.DataFrame(all_symbols_df[all_symbols_df['type'].isin(['et'])]['symbol'].copy(deep=True))
etf_list.reset_index(inplace=True, drop=True)



symbols_base = f"{Path(os.getcwd()).parents[0]}/data/tickers"
etf_list_fname = f"{symbols_base}/etf_list.gz"
etf_list.to_json(etf_list_fname, compression='gzip')

etf_df = pd.read_json(etf_list_fname, compression='gzip')

"""



# %% codecell
######################################################
