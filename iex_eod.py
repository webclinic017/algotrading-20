"""
Analyze IEX End of Day Quotes.
"""
# %% codecell
##################################
import requests
import pandas as pd
import numpy as np
import sys
from datetime import date
import os
import importlib
from dotenv import load_dotenv
from io import StringIO, BytesIO
from json import JSONDecodeError
from datetime import date

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

#from data_collect.iex_routines import iexClose
#importlib.reload(sys.modules['data_collect.iex_routines'])
#from data_collect.iex_routines import iexClose

from multiuse.help_class import baseDir, dataTypes, getDate, local_dates
from data_collect.iex_class import readData, urlData
# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################

iex_eod = serverAPI('iex_quotes_raw')
iex_df = iex_eod.df.T.copy(deep=True)

iex_df.shape

iex_df.reset_index(drop=True, inplace=True)

iex_df.info(memory_usage='deep')

iex_times = pd.to_datetime(iex_df['closeTime'], unit='ms')
iex_times.value_counts(ascending=False).head(500)

iex_times.max()

iex_df.head(10)

# %% codecell
##################################

iex_close = iexClose()

len(iex_close.get.content)
iex_close.url

a_today = pd.DataFrame(iex_close.get.json(), index=range(1))
iex_close.get.content

year = date.today().year
sym = 'A'

fpath_base = f"{baseDir().path}/iex_eod_quotes"
fpath = f"{fpath_base}/{year}/{sym.lower()[0]}/_{sym}.gz"


exist = pd.DataFrame([pd.read_json(fpath, compression='gzip', typ='series')])
exist
exist.head(10)

# %% codecell
##################################

iex_close = iexClose()


# %% codecell
##################################

# GET /stock/{symbol}/quote/{field}


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
        try:
            df_all_syms = pd.read_json(all_symbols_fpath)
        except ValueError:
            df_all_syms = readData.get_all_symbols()

        df_all_syms = dataTypes(df_all_syms).df

        self.symbols = list(df_all_syms['symbol'])

    @classmethod
    def start_quote_process(cls, self):
        """Where the for loop for getting and updating data starts."""
        year = date.today().year

        for sym in self.symbols:
            try:
                self._get_update_local(self, sym, year)
            except JSONDecodeError:
                pass

    @classmethod
    def _get_update_local(cls, self, sym, year):
        """Get quote data, update fpath, upate gzip, write to gzip."""
        self.url = f"{self.base_url}/stock/{sym}/quote"
        get = requests.get(self.url, params=self.payload)
        existing, new_data = '', ''
        try:
            new_data = pd.DataFrame(get.json(), index=range(1))
        except JSONDecodeError:
            return

        fpath = f"{self.fpath_base}/{year}/{sym.lower()[0]}/_{sym}.gz"

        existing = pd.DataFrame()
        try:
            existing = pd.DataFrame([pd.read_json(fpath, compression='gzip', typ='series')])
        except ValueError as ve:
            pass
        except FileNotFoundError:
            pass

        new_df = pd.concat([existing, new_data])
        new_df.reset_index(drop=True, inplace=True)
        new_df.to_json(fpath, compression='gzip')



# %% codecell
##################################
