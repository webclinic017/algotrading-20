"""
Daily IEX data requests to run.
"""
# %% codecell
##############################################
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
    from scripts.dev.help_class import baseDir, dataTypes, getDate
    from scripts.dev.iex_class import readData, urlData
except ModuleNotFoundError:
    from help_class import baseDir, dataTypes, getDate
    importlib.reload(sys.modules['help_class'])
    from help_class import baseDir, dataTypes, getDate

    from iex_class import readData, urlData
    importlib.reload(sys.modules['iex_class'])
    from iex_class import readData, urlData

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
        self.write_to_json(self)

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
        iex_sup.drop(columns=
                     ['exchangeSuffix', 'exchangeName',
                      'name', 'iexId', 'region',
                      'currency', 'isEnabled', 'cik', 'lei', 'figi'],
                     axis=1, inplace=True)
        # Convert data types
        iex_sup = dataTypes(iex_sup).df
        new_syms_tp = iex_sup[iex_sup['symbol'].isin(self.new_syms['symbol'])]
        new_syms_tp.reset_index(inplace=True, drop=True)
        return new_syms_tp

    @classmethod
    def write_to_json(cls, self):
        """Write new symbols json file to local json file."""
        date_to_use = getDate().query('occ')
        syms_fname = f"{baseDir().path}/tickers/new_symbols/{date_to_use}.gz"
        self.new_syms_tp.to_json(syms_fname, compression='gzip')

# %% codecell
##############################################


# %% codecell
##############################################
