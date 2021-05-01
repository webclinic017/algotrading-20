"""
Implement IEX data requests
"""
# %% codecell
######################################################
import os
import os.path

import json
from json import JSONDecodeError
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
    from scripts.dev.multiuse.help_class import baseDir, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate

# %% codecell
######################################################


class readData():
    """All purpose class to read local IEX data."""

    @staticmethod
    def get_all_symbols():
        """Get and write to local file all symbols."""
        syms_fpath = f"{baseDir().path}/tickers/all_symbols.gz"
        symbols = urlData("/ref-data/symbols").df
        symbols.to_json(syms_fpath, compression='gzip')
        return symbols

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
        if os.path.isfile(etf_fname):
            etf_df = pd.read_json(etf_fname, compression='gzip')
        else:
            symbols = urlData("/ref-data/symbols").df.copy(deep=True)
            etf_df = pd.DataFrame(symbols[symbols['type'] == 'et']['symbol'])
            etf_df.reset_index(inplace=True, drop=True)
            etf_df.to_json(etf_fname, compression='gzip')
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
        #print('Data accessible by xxx.df')
        self.df = self._get_data(self, url_suf)

    @classmethod
    def _get_data(cls, self, url_suf):
        """Get data from IEX, return dataframe."""
        load_dotenv()
        base_url = os.environ.get("base_url")
        payload = {'token': os.environ.get("iex_publish_api")}
        get = requests.get(f"{base_url}{url_suf}", params=payload)
        self.get = get.content

        # Set empty dataframe variable
        df = ''
        try:
            df = pd.read_json(BytesIO(get.content))
        except ValueError:
            try:
                df = pd.json_normalize(StringIO(get.content.decode('utf-8')))
            except JSONDecodeError or AttributeError:
                df = pd.read_csv(BytesIO(get.content), escapechar='\n', delimiter=',')
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
class marketHolidays():
    """Helper class for finding last trading date/market holidays."""
    # which can be 'trade' or 'holiday'
    # Dataframe is stored as days_df
    base_dir = f"{baseDir().path}/economic_data"

    def __init__(self, which):
        self.fpath = self.determine_fpath(self, which)
        self.days_df = self.check_if_exists(self)

        if isinstance(self.days_df, (bool)):
            self.params = self.determine_params(which)
            self.full_url = self.construct_url(self.params)
            self.days_df = self.get_market_trading_dates(self)
            self.write_to_json(self)

    @classmethod
    def determine_fpath(cls, self, which):
        """Determine fpath based on which param."""
        fpath = ''

        if which == 'trade':
            fpath = f"{self.base_dir}/{date.today()}_{'trade'}.gz"
        elif which == 'holiday':
            fpath = f"{self.base_dir}/{date.today().year}_{'holiday'}.gz"
        else:
            print("Which needs to equal either 'trade' or 'holiday' ")

        return fpath

    @classmethod
    def check_if_exists(cls, self):
        """Check if requested file exists locally."""
        if os.path.isfile(self.fpath):
            local_df = pd.read_json(self.fpath, compression='gzip')
        else:
            local_df = False

        return local_df


    @classmethod
    def determine_params(cls, which):
        """Create dictionary of parameters."""
        params = {'type': which, 'direction': '', 'last': 0, 'startDate': 'YYYYMMDD'}
        if which == 'trade':
            params['direction'] = 'last'
            params['last'] = 1
        elif which == 'holiday':
            params['direction'] = 'next'
            next_year = (date.today() + timedelta(days=365)).year
            params['last'] = (datetime.date(next_year, 1, 1,) - date.today()).days
        else:
            return False

        params['startDate'] = date.today().strftime("%Y%m%d")
        return params

    @classmethod
    def construct_url(cls, params):
        """Construct url to call get request."""
        load_dotenv()
        base_url = os.environ.get("base_url")
        url_1 = f"/ref-data/us/dates/{params['type']}/{params['direction']}"
        url_2 = f"/{params['last']}/{params['startDate']}"
        full_url = f"{base_url}{url_1}{url_2}"

        return full_url

    @classmethod
    def get_market_trading_dates(cls, self):
        """Get market holiday data from iex."""
        payload = {'token': os.environ.get("iex_publish_api")}
        td_get = requests.get(
            self.full_url,
            params=payload  # Passed through function arg
            )
        # Convert bytes to dataframe=
        df = pd.json_normalize(json.loads(td_get.content))

        return df

    @classmethod
    def write_to_json(cls, self):
        """Write data to local json file."""
        self.days_df.to_json(self.fpath, compression='gzip')
# %% codecell
######################################################
class companyStats():
    """Static methods for various company data."""

    stats_dict = {
        'advanced_stats': {'url_suffix': 'advanced-stats',
                           'local_fpath': 'key_stats'},
        'fund_ownership': {'url_suffix': 'fund-ownership',
                           'local_fpath': 'fund_ownership'},
        'insiders': {'url_suffix': 'insider-roster',
                     'local_fpath': 'insiders'},
        'insider_trans': {'url_suffix': 'insider-transactions',
                          'local_fpath': 'insider_trans'},
        'institutional_owners': {'url_suffix': 'institutional-ownership',
                                 'local_fpath': 'institutional_owners'},
        'peers': {'url_suffix': 'peers',
                  'local_fpath': 'peers'},
    }

    def __init__(self, symbols, which):
        self.get_fname(self, which)
        self.df = self.check_local(self, symbols, which)

    @classmethod
    def get_fname(cls, self, which):
        """Get local fname to use."""
        base_dir = f"{baseDir().path}/company_stats"
        self.fpath = f"{base_dir}/{self.stats_dict[which]['local_fpath']}"

    @classmethod
    def check_local(cls, self, symbols, which):
        """Check for local data before requesting from IEX."""
        which = 'fund_ownership'
        base_dir = f"{baseDir().path}/company_stats"
        all_df = pd.DataFrame()

        for sym in symbols:
            full_path = f"{base_dir}/{self.stats_dict[which]['local_fpath']}/{sym[0].lower()}/*"
            data_today = f"{full_path}_{date.today()}"
            data_yest = f"{full_path}_{date.today() - timedelta(days=1)}"

            if os.path.isfile(data_today):
                df = pd.read_json(data_today, compression='gzip')
                all_df = pd.concat([all_df, df])
            elif os.path.isfile(data_yest):
                df = pd.read_json(data_yest, compression='gzip')
                all_df = pd.concat([all_df, df])
            else:
                df = self._get_data(self, sym, which)
                all_df = pd.concat([all_df, df])

        all_df.reset_index(inplace=True, drop=True)
        return all_df

    @classmethod
    def _get_data(cls, self, sym, which):
        """Base function for getting company stats data."""
        url = f"/stock/{sym}/{self.stats_dict[which]['url_suffix']}"
        df = urlData(url).df
        path = Path(f"{self.fpath}/{sym[0].lower()}/_{sym}_{date.today()}.gz")
        df.to_json(path, compression='gzip')
        return df

# %% codecell
######################################################
