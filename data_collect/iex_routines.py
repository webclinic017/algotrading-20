"""Daily IEX data requests to run."""
# %% codecell
##############################################
import os
import os.path
from pathlib import Path
from time import sleep
from tqdm import tqdm

# import json
from json import JSONDecodeError
from io import StringIO
# import gzip
import importlib
import sys
import glob

import pandas as pd
# import numpy as np
import requests
from requests.exceptions import SSLError
from dotenv import load_dotenv
# from pathlib import Path

import datetime
from datetime import date, timedelta, time

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate, local_dates, help_print_arg, write_to_parquet
    from scripts.dev.data_collect.iex_class import readData, urlData
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate, local_dates, help_print_arg, write_to_parquet
    from data_collect.iex_class import readData, urlData
    from api import serverAPI

# %% codecell
##############################################

# %% codecell
##############################################


def otc_ref_data():
    """Get all otc reference data from IEX cloud."""
    # Use urlData function to get all otc sym ref data
    otc_syms = urlData('/ref-data/otc/symbols').df
    # Create fpath to store otc_syms
    fpath = f"{baseDir().path}/tickers/symbol_list/otc_syms.parquet"
    # Write otc symbols to local gzip file
    write_to_parquet(otc_syms, fpath)


def get_company_meta_data():
    """Get company meta data, save locally, from IEX."""
    all_symbols = serverAPI('all_symbols').df
    all_cs = all_symbols[all_symbols['type'].isin(['cs', 'ad'])]
    sym_list = all_cs['symbol'].unique().tolist()

    bpath = Path(baseDir().path, 'company_stats', 'meta')

    for sym in tqdm(sym_list):
        try:
            ud = urlData(f"/stock/{sym}/company")
            fpath_suf = f"{sym.lower()[0]}/_{sym}.parquet"
            fpath = bpath.joinpath(fpath_suf)
            write_to_parquet(ud.df, fpath)
        except Exception as e:
            print(f"Company meta stats error: {type(e)} {str(e)}")


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
        new_syms = (current_syms[(current_syms['symbol'].isin(syms_diff))
                                 & (current_syms['isEnabled'])])
        # Ignore cryptocurrency pairs
        new_syms = new_syms[~new_syms['symbol'].str[-4:].isin(['USDT'])]

        return new_syms

    @classmethod
    def new_syms_ref_type(cls, self):
        """Get reference type data for new symbols."""
        iex_sup = urlData("/ref-data/symbols").df
        fpath = f"{baseDir().path}/tickers/all_symbols.parquet"
        # Write to parquet file
        write_to_parquet(iex_sup, fpath)

        iex_sup.drop(columns=['exchangeSuffix', 'exchangeName',
                              'name', 'iexId', 'region',
                              'currency', 'isEnabled', 'cik', 'lei', 'figi'],
                     axis=1, inplace=True)

        new_syms_tp = iex_sup[iex_sup['symbol'].isin(self.new_syms['symbol'])]
        new_syms_tp.reset_index(inplace=True, drop=True)

        dt = getDate().query('occ')
        fpath_new = f"{baseDir().path}/tickers/new_symbols/{dt}.parquet"
        write_to_parquet(new_syms_tp, fpath_new)

        return new_syms_tp

# %% codecell
##############################################
# Data schedule: 4:30 AM - 8:00 PM Monday-Friday
# Data weight - 1. For ~ 10,000 symbols, this is 10,000 credits.
# March 5th is the first day this was ran


class iexClose():
    """Get end of day quotes for all symbols."""

    fpath_base = f"{baseDir().path}/iex_eod_quotes"
    data_list = []

    def __init__(self, otc=False):
        self.get_params(self)
        self.symbols = self.get_all_symbols(self, otc)
        self.start_quote_process(self)
        self.combine_write(self)

    @classmethod
    def get_params(cls, self):
        """Get payload/base_url params."""
        load_dotenv()
        self.base_url = os.environ.get("base_url")
        self.payload = {'token': os.environ.get("iex_publish_api")}

    @classmethod
    def get_all_symbols(cls, self, otc):
        """Get list of all IEX supported symbols (9000 or so)."""
        all_symbols_fpath = ''
        bpath = Path(baseDir().path, 'tickers', 'symbol_list')

        if otc:
            all_symbols_fpath = bpath.joinpath('otc_syms.parquet')
        else:
            all_symbols_fpath = bpath.joinpath('all_symbols.parquet')

        try:
            df_all_syms = pd.read_parquet(all_symbols_fpath)
        except ValueError:
            df_all_syms = readData.get_all_symbols()

        return list(df_all_syms['symbol'])

    @classmethod
    def start_quote_process(cls, self):
        """Where the for loop for getting and updating data starts."""
        year = date.today().year

        for sn, sym in enumerate(self.symbols):
            try:
                self._get_update_local(self, sym, year)
            except Exception as e:
                print(e)

    @classmethod
    def _get_update_local(cls, self, sym, year, new_data=''):
        """Get quote data, update fpath, upate gzip, write to gzip."""
        self.url = f"{self.base_url}/stock/{sym}/quote"
        get = requests.get(self.url, params=self.payload)

        try:
            new_data = pd.DataFrame(get.json(), index=range(1))
            self.data_list.append(new_data)
        except JSONDecodeError:
            return

        fpath = f"{self.fpath_base}/{year}/{sym.lower()[0]}/_{sym}.parquet"
        write_to_parquet(new_data, fpath, combine=True)

    @classmethod
    def combine_write(cls, self):
        """Concat and write to local combined df."""
        all_df = pd.concat(self.data_list)
        all_df.reset_index(drop=True, inplace=True)
        # Get date for data to use for fpath
        latest_dt = pd.to_datetime(
            all_df['latestUpdate'], unit='ms').dt.date[0]
        # Construct fpath
        fpath = f"{self.fpath_base}/combined/_{latest_dt}.parquet"
        # Minimize file size and write to parquet
        write_to_parquet(all_df, fpath)


# %% codecell
##############################################


def write_combined():
    """Concat iex eod prices into one file."""
    base_dir = baseDir().path
    fpath = f"{base_dir}/iex_eod_quotes/{date.today().year}/*/**.parquet"
    choices = glob.glob(fpath)

    concat_list = []
    for choice in choices:
        concat_list.append(pd.read_parquet(choice))

    all_df = pd.concat(concat_list)
    this_df = all_df.copy(deep=True)
    this_df['date'] = pd.to_datetime(
        this_df['latestUpdate'], unit='ms').dt.date
    cutoff = datetime.date(2021, 4, 7)
    this_df = this_df[this_df['date'] >= cutoff].copy(deep=True)

    this_df.sort_values(by=['symbol', 'latestUpdate'],
                        inplace=True, ascending=False)
    this_df.drop_duplicates(subset=['symbol', 'date'], inplace=True)

    dt_counts = this_df['date'].value_counts().index
    for dt in dt_counts:
        mod_df = this_df[this_df['date'] == dt]
        mod_df.reset_index(inplace=True, drop=True)
        mod_fpath = f"{base_dir}/iex_eod_quotes/combined/_{dt}.parquet"
        write_to_parquet(mod_df, mod_fpath)


# %% codecell
##############################################


def get_sector_performance(drop_dup=False):
    """Run each business day, every 60 minutes, from market open."""

    iex_df = urlData("/stock/market/sector-performance").df
    iex_df['perf_perc'] = (iex_df['performance'] * 100).round(1)

    # Create a new column for the date
    last_date = (pd.to_datetime(iex_df['lastUpdated'].iloc[0],
                                unit='ms').date())
    iex_df['date'] = last_date
    # Create a new column for hour
    last_hour = (pd.to_datetime(iex_df['lastUpdated'].iloc[0], unit='ms').hour)
    iex_df['hour'] = last_hour

    # Read local data and concatenate
    bpath = Path(baseDir().path, 'tickers', 'sectors')
    fpath = bpath.joinpath(f"performance_{last_date}.parquet")

    write_to_parquet(iex_df, fpath, combine=True)

# %% codecell
##############################################


class histPrices():
    """Class for historical price data."""

    base_dir = baseDir().path
    full_hist = False
    # period can equal 'ytd', 'previous', 'date'
    # replace is a boolean value: False for testing

    # sym_list is literally just a list of symbols
    # Originated from the stocktwits trending data symbols

    # ** If sym_list is empty, then update all local syms **

    def __init__(self, sym_list, date_range='ytd'):
        self.date_range = date_range
        self.get_params(self)
        if self.full_hist:
            self.get_full_hist(self, sym_list)
        else:
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
        # Determine if local dates/ytd data should be pulled.
        if self.date_range in ('1y', '2y', '5y'):
            self.full_hist = True

    @classmethod
    def get_local_dates(cls, self, sym_list):
        """Get the dict of all revelant dates needed."""
        self.ld_dict = local_dates('StockEOD', sym_list)
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
        # df_dl = pd.DataFrame()
        df_dl, df_all = [], ''
        # Call get request for individual dates, append to df
        per = 'date'
        for day in str_dates:
            # Get data and append to previous dataframe
            df_dl.append(self.get_hist(self, sym, per, day))
        try:
            df_dl = [x for x in df_dl if x]
            if len(df_dl) > 0:
                # Combine previous dataframes with new data
                df_all = pd.concat(
                    [pd.read_parquet(path), df_dl])
            else:
                df_all = pd.read_parquet(path)
            df_all.reset_index(inplace=True, drop=True)
            # Write to local parquet file
            write_to_parquet(df_all, path)
        except TypeError:
            self.df_dl = df_dl
            print('Except TypeError. Check class.df_dl for var df_dl')

    @classmethod
    def for_ytd_dates(cls, self):
        """Get ytd dates for dates in self.get_ytd_syms from __init__."""
        # Get the year to use in file path
        # Set period to year-to-date and dt(date) to empty string
        dt = ''
        base_path = f"{self.base_dir}/StockEOD/{date.today().year}"

        for sym in self.get_ytd_syms:
            try:
                path = f"{base_path}/{sym.lower()[0]}/_{sym}.parquet"
                # print(f"ytd-path:{path}")
                df = self.get_hist(self, sym, self.date_range, dt)
                write_to_parquet(df, path)
            except ValueError:
                self.json_errors.append(sym)
            except JSONDecodeError:
                self.json_errors.append(sym)

    @classmethod
    def get_hist(cls, self, sym, per, dt):
        """Get historical data for date/dates."""
        data = ''
        if (per == 'date') and (dt != ''):
            url = f"{self.base_url}/stock/{sym}/chart/date/{dt}"
            # print(url)
            data = requests.get(url, params=self.payload).json()
        # If individual dates are not getting called
        elif (per != 'date') and (dt == ''):
            url = f"{self.base_url}/stock/{sym}/chart/{per}"
            get = requests.get(url, params=self.payload)
            data = pd.read_json(StringIO(get.content.decode('utf-8')))

        return data

    @classmethod
    def get_full_hist(cls, self, sym_list):
        """Get full history with the date_range param."""
        for sym in sym_list:
            url = f"{self.base_url}/stock/{sym}/chart/{self.date_range}"
            df = pd.DataFrame(requests.get(url, params=self.payload).json())

            df['year'] = pd.to_datetime(df['date']).dt.year
            years = df['year'].value_counts().index.tolist()
            for yr in years:
                df_mod = df[df['year'] == yr].copy(deep=True)
                df_mod.drop(columns=['year'], inplace=True)
                df_mod.reset_index(drop=True, inplace=True)
                df_mod = dataTypes(df_mod).df
                fpath = f"{self.base_dir}/StockEOD/{yr}/{sym.lower()[0]}/_{sym}.parquet"
                write_to_parquet(df_mod, fpath)


# %% codecell
###############################################################


class IexOptionSymref():
    """Get iex options symref data, write to parquet."""
    fpath = ''

    def __init__(self, sym):
        self.construct_fpath(self, sym)
        df_raw = self.get_data(self, sym)

        self.concat_data(self, df_raw)
        self.write_to_parquet(self)

    @classmethod
    def construct_fpath(cls, self, sym):
        """Construct local fpath."""
        base_dir, yr = baseDir().path, date.today().year
        fpath_base = f"{base_dir}/derivatives/iex_symref/{yr}"
        fpath = f"{fpath_base}/{sym.lower()[0]}/_{sym}.parquet"
        self.fpath = fpath

    @classmethod
    def get_data(cls, self, sym):
        """Construct parameters."""
        load_dotenv()
        base_url = os.environ.get("base_url")
        payload = {'token': os.environ.get("iex_publish_api")}
        url = f"{base_url}/ref-data/options/symbols/{sym}"

        get = requests.get(url, params=payload)
        df = pd.DataFrame(get.json())
        return df

    @classmethod
    def concat_data(cls, self, df):
        """Concat data with previous file if it exists."""

        if os.path.isfile(self.fpath):
            df_all = pd.concat([pd.read_parquet(self.fpath), df])
            df_all = (df_all.drop_duplicates(subset='symbol')
                            .reset_index(drop=True))
            df = dataTypes(df_all).df.copy(deep=True)
        else:
            # Minimize size of data
            df = dataTypes(df).df.copy(deep=True)

        self.df = df

    @classmethod
    def write_to_parquet(cls, self):
        """Write data to local parquet file."""
        write_to_parquet(self.df, self.fpath)
