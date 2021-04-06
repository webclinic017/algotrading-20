"""
Daily IEX data requests to run.
"""
# %% codecell
##############################################
import os
import os.path

# import json
from json import JSONDecodeError
from io import StringIO
# import gzip
import importlib
import sys

import pandas as pd
# import numpy as np
import requests
from requests.exceptions import SSLError
from dotenv import load_dotenv
# from pathlib import Path

import datetime
from datetime import date, timedelta, time

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate, local_dates
    from scripts.dev.data_collect.iex_class import readData, urlData
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate, local_dates
    importlib.reload(sys.modules['multiuse.help_class'])
    from multiuse.help_class import baseDir, dataTypes, getDate, local_dates

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

        for sn, sym in enumerate(self.symbols):
            try:
                self._get_update_local(self, sym, year)
            except JSONDecodeError:
                pass
            except SSLError:
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

        existing = ''
        try:
            # existing = pd.DataFrame([pd.read_json(fpath, compression='gzip')])
            existing = pd.read_json(fpath, compression='gzip')
        except FileNotFoundError as fe:
            print(fe)
            existing = pd.DataFrame()
        except ValueError as ve:
            print(ve)
            existing = pd.DataFrame()

        try:
            new_df = pd.concat([existing, new_data])
            new_df.reset_index(drop=True, inplace=True)
            new_df.to_json(fpath, compression='gzip')
        except ValueError as ve:
            print(ve)
            pass

# %% codecell
##############################################


class histPrices():
    """Class for historical price data."""

    base_dir = baseDir().path
    # period can equal 'ytd', 'previous', 'date'
    # replace is a boolean value: False for testing

    # sym_list is literally just a list of symbols
    # Originated from the stocktwits trending data symbols

    def __init__(self, sym_list):
        self.get_params(self)
        self.get_local_dates(self, sym_list)
        # Update missing individual dates
        self.for_ind_dates(self)
        # Get ytd data for syms with no local data
        self.for_ytd_dates(self)

    @classmethod
    def get_params(cls, self):
        """Get params that are used later on."""
        load_dotenv()
        self.base_url = os.environ.get("base_url")
        self.payload = {'token': os.environ.get("iex_publish_api")}
        self.json_errors = []

    @classmethod
    def get_local_dates(cls, self, sym_list):
        """Get the dict of all revelant dates needed."""
        self.ld_dict = local_dates('StockEOD')
        # Get all the syms that are not saved locally
        not_local_syms = ([sym for sym in sym_list
                           if sym not in self.ld_dict['syms_list']])
        self.get_ytd_syms = not_local_syms

    @classmethod
    def for_ind_dates(cls, self):
        """For loop initiation for getting individual dates."""
        sort_dates = sorted(self.ld_dict['dl_ser'])
        dt_keys = self.ld_dict['syms_dict_to_get'].keys()
        dt_vals = self.ld_dict['syms_dict_to_get'].values()
        zipped_for_dates = zip(dt_keys, dt_vals, self.ld_dict['syms_fpaths'])

        # For all the missing local dates
        for sym, dates, path in zipped_for_dates:
            # If none of the necessary dates are stored locally, get ytd
            if sorted(dates) == sort_dates:
                self.get_ytd_syms.append(sym)
            else:  # If only some of the dates match
                self.else_ind_dates(self, sym, dates, path)

    @classmethod
    def else_ind_dates(cls, self, sym, dates, path):
        """Else part of previous for loop ^^."""
        str_dates = pd.to_datetime(dates).dt.strftime("%Y%m%d")
        df_dl = pd.DataFrame()
        # Call get request for individual dates, append to df
        per = 'date'
        for day in str_dates:
            # Get data and append to previous dataframe
            df_dl = df_dl.append(self.get_hist(self, sym, per, day))
            # Combine previous dataframe with new data
            df_all = pd.concat([pd.read_json(path), df_dl])
            df_all.reset_index(inplace=True, drop=True)
            # Write to local json file
            df_all.to_json(path, compression='gzip')

    @classmethod
    def for_ytd_dates(cls, self):
        """Get ytd dates for dates in self.get_ytd_syms."""
        # Get the year to use in file path
        # Set period to year-to-date and dt(date) to empty string
        per, dt = 'ytd', ''
        base_path = f"{self.base_dir}/StockEOD/{date.today().year}"

        for sym in self.get_ytd_syms:
            try:
                path = f"{base_path}/{sym.lower()[0]}/_{sym}.gz"
                df = self.get_hist(self, sym, per, dt)
                df.to_json(path, compression='gzip')
            except ValueError:
                self.json_errors.append(sym)

    @classmethod
    def get_hist(cls, self, sym, per, dt):
        """Get historical data for date/dates."""
        data = ''
        if (per == 'date') and (dt != ''):
            url = f"{self.base_url}/stock/{sym}/chart/date/{dt}"
            data = requests.get(url, params=self.payload).json()
        elif (per == 'ytd') and (dt == ''):
            url = f"{self.base_url}/stock/{sym}/chart/ytd"
            get = requests.get(url, params=self.payload)
            data = pd.read_json(StringIO(get.content.decode('utf-8')))

        return data
