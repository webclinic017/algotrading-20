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
    from dev.help_class import baseDir
except ModuleNotFoundError:
    from help_class import baseDir
# %% codecell
######################################################

class readData():
    """All purpose class to read local IEX data."""

    @staticmethod
    def all_iex_symbols():
        """Read all IEX symbols."""
        symbols_base = f"{baseDir().path}/data/tickers"
        all_symbols_fname = f"{symbols_base}/all_symbols.gz"
        ticker_df = pd.read_json(all_symbols_fname, compression='gzip')
        return ticker_df


    @staticmethod
    def etf_list():
        """Read local etf list."""
        etf_fname = f"{baseDir().path}/data/tickers/etf_list.gz"
        etf_df = pd.read_json(etf_fname, compression='gzip')
        return etf_df

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
