"""
Daily IEX data requests to run.
"""
# %% codecell
##############################################
import os
import os.path

import json
from io import StringIO, BytesIO
import gzip
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
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate
    from scripts.dev.data_collect.iex_class import readData, urlData
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate
    importlib.reload(sys.modules['multiuse.help_class'])
    from multiuse.help_class import baseDir, dataTypes, getDate

    from data_collect.iex_class import readData, urlData
    importlib.reload(sys.modules['data_collect.iex_class'])
    from data_collect.iex_class import readData, urlData

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##############################################

# %% codecell
##############################################

class dailySymbols():
    """Get a list of all symbols supported by IEX."""

    def __init__(self):
        self.new_syms = self.get_merge_sort(self)
        self.new_syms_tp = self.new_syms_ref_type(self)

    @classmethod
    def get_merge_sort(cls, self):
        """Get new symbols and find diff with last date."""
        current_syms = urlData("/ref-data/iex/symbols").df

        old_syms = readData.last_bus_day_syms()
        old_syms['symbol'] = ''

        # Get the differennce between todays/yesterdays symbols
        syms_diff = (pd.concat([current_syms['symbol'], old_syms['symbol']])
                       .drop_duplicates(keep=False))
        # New symbols from last time checking symbols (yesterday)
        new_syms = (current_syms[(current_syms['symbol'].isin(syms_diff)) &
                                 (current_syms['isEnabled'])])
        # Ignore cryptocurrency pairs
        new_syms = new_syms[~new_syms['symbol'].str[-4:].isin(['USDT'])]

        return new_syms

    @classmethod
    def new_syms_ref_type(cls, self):
        """Get reference type data for new symbols."""
        iex_sup = urlData("/ref-data/symbols").df
        syms_fname = f"{baseDir().path}/tickers/all_symbols.gz"
        self.write_to_json(self, iex_sup, syms_fname)

        iex_sup.drop(columns=
                     ['exchangeSuffix', 'exchangeName',
                      'name', 'iexId', 'region',
                      'currency', 'isEnabled', 'cik', 'lei', 'figi'],
                     axis=1, inplace=True)
        # Convert data types
        iex_sup = dataTypes(iex_sup).df
        new_syms_tp = iex_sup[iex_sup['symbol'].isin(self.new_syms['symbol'])]
        new_syms_tp.reset_index(inplace=True, drop=True)

        date_to_use = getDate().query('occ')
        syms_fname = f"{baseDir().path}/tickers/new_symbols/{date_to_use}.gz"
        self.write_to_json(self, new_syms_tp, syms_fname)

        return new_syms_tp

    @classmethod
    def write_to_json(cls, self, df, syms_fname):
        """Write new symbols json file to local json file."""
        df.to_json(syms_fname, compression='gzip')

# %% codecell
##############################################
# Data schedule: 4:30 AM - 8:00 PM Monday-Friday
# Data weight - 1. For ~ 10,000 symbols, this is 10,000 credits.
# March 5th is the first day this was ran

class iexClose():
    """Get end of day quotes for all symbols."""

    fpath_base = f"{baseDir().path}/iex_eod_quotes"

    def __init__(self):
        self.get_params(self)
        self.get_all_symbols(self)
        self.start_quote_process(self)

    @classmethod
    def get_params(cls, self):
        """Get payload/base_url params."""
        load_dotenv()
        self.base_url = os.environ.get("base_url")
        self.payload = {'token': os.environ.get("iex_publish_api")}

    @classmethod
    def get_all_symbols(cls, self):
        """Get list of all IEX supported symbols (9000 or so)."""
        all_symbols_fpath = f"{baseDir().path}/tickers/all_symbols.gz"
        df_all_syms = pd.read_json(all_symbols_fpath)
        df_all_syms = dataTypes(df_all_syms).df

        self.symbols = list(df_all_syms['symbol'])

    @classmethod
    def start_quote_process(cls, self):
        """Where the for loop for getting and updating data starts."""
        year = date.today().year

        for sym in self.symbols:
            self._get_update_local(self, sym, year)

    @classmethod
    def _get_update_local(cls, self, sym, year):
        """Get quote data, update fpath, upate gzip, write to gzip."""
        url = f"{self.base_url}/stock/{sym}/quote"
        get = requests.get(url, params=self.payload)

        fpath = f"{self.fpath_base}/{year}/{sym.lower()[0]}/_{sym}.gz"
        with gzip.open(fpath, 'wb') as f:
            f.write(get.content)

# %% codecell
##############################################
