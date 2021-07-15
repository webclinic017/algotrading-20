"""
Helper classes for routine procedures.
Int8 can store integers from -128 to 127
Int16 can store integers from -32768 to 32767
Int32 can store integers from -2,147,483,648 to 2,147,483,648
Int64

uint8: Unsigned integer (0 to 255)
uint16: Unsigned integer (0 to 65535)
uint32: Unsigned integer (0 to 4294967295)

"""
# %% codecell
###############################################################################
from dotenv import load_dotenv
import glob
import os
import json
from pathlib import Path
from datetime import timedelta, date
import datetime
import pytz
from pandas.tseries.offsets import BusinessDay
from gzip import BadGzipFile

import pandas as pd
import numpy as np
import requests

"""
try:
    from app.tasks_test import print_arg_test
except ModuleNotFoundError:
    pass
"""

# %% codecell
###############################################################################

"""
def help_print_arg(arg):
    Print arg on local or server side
    try:
        print_arg_test.delay(arg)
    except NameError:
        print(arg)
"""


def df_create_bins(df, bin_size=1000):
    """Create bins sizes of default 1000 each. Add to df."""
    if isinstance(df.index, object):
        df.reset_index(drop=True, inplace=True)

    qcut_bin_n = df.shape[0] // 1000
    bin_labels = list(range(1, qcut_bin_n + 1))

    # Create column with bin numbers
    df['bins'] = pd.qcut(df.index.tolist(), q=qcut_bin_n, labels=bin_labels)
    # Return dataframe
    return df


class baseDir():
    """Get the current base directory and adjust accordingly."""
    load_dotenv()
    env = os.environ.get("env")

    def __init__(self):
        if self.env == 'production':
            self.path = f"{Path(os.getcwd())}/data"
        else:
            self.path = f"{Path(os.getcwd()).parents[0]}/data"


class scriptDir():
    """Get the current base directory and adjust accordingly."""
    load_dotenv()
    env = os.environ.get("env")

    def __init__(self):
        if self.env == 'production':
            self.path = f"{Path(os.getcwd())}/scripts/dev/data_collect"
        else:
            self.path = f"{Path(os.getcwd()).parents[0]}/dev/data_collect"


class RWJsonDicts():
    """Read and write local json dictionaries."""

    # When writing, set file arg to writable dict
    # When reading, data accessible by .data attribute
    data = False
    base_dir = baseDir().path
    local_dicts = ({
        'iex_options_symref': f"{base_dir}/derivatives/iex_symref/expos.json"
    })

    def __init__(self, fpath=False, file=False, local_dict=False):
        if local_dict:
            fpath = self.get_local_fpath(self, local_dict)
        if file:
            self.write_dict_to_json(self, fpath, file)
        if not file:
            self.read_dict_from_json(self, fpath)

    @classmethod
    def get_local_fpath(cls, self, local_dict):
        """Get fpath of local json file."""
        fpath = self.local_dicts[local_dict]
        return fpath

    @classmethod
    def write_dict_to_json(cls, self, fpath, file):
        """Write dictionary to json file."""
        with open(fpath, 'w') as write_this:
            json.dump(file, write_this)

    @classmethod
    def read_dict_from_json(cls, self, fpath):
        """Read local json file into dictionary."""
        with open(fpath, 'rb') as read_this:
            self.data = json.load(read_this)

# %% codecell
###############################################################################


class getDate():
    """Get the right query date."""

    @staticmethod
    def time_cutoff(cutoff_hm=9.55):
        """Calculate cutoff with default at 9:30."""
        nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
        nyc_hm = nyc_datetime.hour + (nyc_datetime.minute/60)

        cutoff = False
        if nyc_hm < cutoff_hm:
            cutoff = True
        return cutoff

    @staticmethod
    def which_fname_date():
        """Figure out which date to use for file names."""
        cutoff = getDate.time_cutoff()

        date_today = date.today().weekday()
        weekdays = (0, 1, 2, 3, 4)

        if (cutoff) and (date_today in weekdays):
            da_min = 1
        else:
            da_min = 0

        if date_today == 0:
            days = 3 + da_min
        elif date_today in weekdays:  # Get previous day data
            days = 1 + da_min
        elif date_today == 5:  # Saturday get thursday data
            days = 2
        elif date_today == 6:
            days = 3

        fname_date = (date.today() - timedelta(days=days))

        return fname_date

    @staticmethod
    def query(site):
        """Call which_fname_date but shorter."""
        # query_date = getDate.which_fname_date()
        weekend, query_date = False, ''
        if date.today().weekday() in (5, 6):
            weekend = True
        elif (date.today().weekday() == 0 and
                not getDate.time_cutoff(cutoff_hm=17.15)):
            weekend = True

        if site in ('cboe', 'occ'):
            if getDate.time_cutoff(cutoff_hm=17.15) or weekend:
                query_date = (date.today() - BusinessDay(n=1)).date()
            else:
                query_date = (date.today() - BusinessDay(n=0)).date()
        elif site in ('iex_close'):
            if weekend:
                query_date = (date.today() - BusinessDay(n=1)).date()
            else:
                query_date = (date.today() - BusinessDay(n=0)).date()
        elif site in ('iex_eod'):
            if getDate.time_cutoff(cutoff_hm=16.15) or weekend:
                query_date = (date.today() - BusinessDay(n=1)).date()
            else:
                query_date = (date.today() - BusinessDay(n=0)).date()
        elif site in ('sec_master'):
            if getDate.time_cutoff(cutoff_hm=22.35) or weekend:
                query_date = (date.today() - BusinessDay(n=1)).date()
            else:
                query_date = (date.today() - BusinessDay(n=0)).date()
        elif site in ('last_syms'):
            pass
        return query_date

    @staticmethod
    def busDays(start_dt, end_dt=date.today(), last_data=True):
        """Get a list of all business days."""

        def daterange(date1, date2):
            for n in range(int((date2 - date1).days)+1):
                yield date1 + timedelta(n)

        date_list = []
        for dt in daterange(start_dt, end_dt):
            if dt.weekday() not in (5, 6):
                date_list.append(dt)
                # print(dt.strftime("%Y-%m-%d"))

        if last_data and getDate.time_cutoff(cutoff_hm=17):
            try:
                date_list.remove(date.today())
            except ValueError:
                pass

        return date_list

    @staticmethod
    def get_bus_days():
        """Get all business days. If file, read and return."""
        fpath = f"{baseDir().path}/ref_data/bus_days.gz"
        df = None

        if not os.path.isfile(fpath):
            rh = RecordHolidays().df['date']
            # Record the earliest/latest holiday and use that as range
            dt_min = rh.min().date()
            dt_max = rh.max().date()
            # Convert datetime index to series
            days_df = (pd.DataFrame(pd.bdate_range(dt_min, dt_max),
                                    columns=['date']))
            # Get all business days that are not holidays
            days = days_df[~days_df.isin(rh)]
            days.reset_index(drop=True, inplace=True)
            # Write to local json file
            days.to_json(fpath, compression='gzip')

            df = days.copy(deep=True)
        else:
            df = pd.read_json(fpath, compression='gzip')

        return df

# %% codecell
###############################################################################


class dataTypes():
    """Helper class for implementing data type conversions."""

    def __init__(self, df):
        if isinstance(df, pd.DataFrame):
            self.dtypes = df.dtypes
            self.df = df.copy(deep=True)
            self._cols_to_cat(self)
            self.pos_or_neg_ints(self)
            self.pos_or_neg_floats(self)
            print('Modified dataframe accessible with xxx.df')
        else:
            self.df = df

    @classmethod
    def _cols_to_cat(cls, self):
        """Convert object columns to categorical."""
        cols_to_cat = self.dtypes[self.dtypes == 'object'].index.to_list()
        self.df[cols_to_cat] = self.df[cols_to_cat].astype('category')

    @classmethod
    def pos_or_neg_ints(cls, self):
        """Convert integers to correct data type."""
        cols_int64 = self.dtypes[self.dtypes == 'int64'].index.to_list()
        int8_val = 127
        int16_val = 32767
        int32_val = 2147483648

        for col in cols_int64:
            min = self.df[col].min()
            max = self.df[col].max()
            if min >= 0:
                if max < 255:
                    self.df[col] = self.df[col].astype(np.uint8)
                elif max < 65535:
                    self.df[col] = self.df[col].astype(np.uint16)
                elif max < 4294967295:
                    self.df[col] = self.df[col].astype(np.uint32)
            else:
                if max < int8_val and min > -int8_val:
                    self.df[col] = self.df[col].astype(np.int8)
                elif max < int16_val and min > -int16_val:
                    self.df[col] = self.df[col].astype(np.int16)
                elif max < int32_val and min > -int32_val:
                    self.df[col] = self.df[col].astype(np.int32)

    @classmethod
    def pos_or_neg_floats(cls, self):
        """Convert floats to correct data type."""
        cols_float64 = self.dtypes[self.dtypes == 'float64'].index.to_list()
        for col in cols_float64:
            _min = self.df[col].min()
            _max = self.df[col].max()
            if _min > np.finfo('float16').min and _max < np.finfo('float16').max:
                self.df[col] = self.df[col].astype(np.float16)
            elif _min > np.finfo('float32').min and _max < np.finfo('float32').max:
                self.df[col] = self.df[col].astype(np.float32)
            else:
                pass


# %% codecell
###############################################################################


def local_dates(which, sym_list):
    """Get dict of missing local dates."""
    stock_suf = ({
        'StockEOD': f"/StockEOD/{date.today().year}/*/**"
    })

    stock_fpath = f"{baseDir().path}{stock_suf[which]}"
    # Min date for stocktwits trending data
    st_date_min = datetime.date(2021, 2, 19)
    dl_ser = pd.Series(getDate.busDays(st_date_min))

    # Get fpaths for all local syms
    local_stock_data = sorted(glob.glob(stock_fpath))

    local_syms = []  # Create an empty list
    for st in local_stock_data:  # Split strings and store symbol names
        local_syms.append(st.split('_')[1][:-3])

    # If the length of the list of symbols > 0, only use those symbols.
    if len(sym_list) > 0:
        # Create sets of both the local syms and the arg sym_list
        sym_list_set, local_syms_set = set(sym_list), set(local_syms)
        # Get the intersection of the sets, then convert to a list
        local_syms_ = sym_list_set.intersection(local_syms_set)
        local_syms = list(local_syms_)

    syms_dict, syms_dict_to_get = {}, {}
    syms_not_get, error = [], False
    dl_ser_dt = pd.to_datetime(dl_ser)
    for st, path in zip(local_syms, local_stock_data):
        try:
            syms_dict[st] = pd.read_json(path, compression='gzip')
        except BadGzipFile:
            syms_dict[st] = pd.read_json(path)
        except:
            error = True
        finally:
            if error:
                os.remove(path)

        # syms_dict[st]['date'] = pd.to_datetime(syms_dict[st]['date'], unit='ms')
        try:
            syms_dict_to_get[st] = dl_ser[~dl_ser_dt.isin(syms_dict[st]['date'].astype('object'))]
        except KeyError:
            syms_dict_to_get[st] = dl_ser
            syms_not_get.append(st)

    ld_dict = {}
    ld_dict['dl_ser'] = dl_ser  # Series of date list
    ld_dict['syms_list'] = local_syms
    ld_dict['syms_cant_get'] = syms_not_get
    ld_dict['syms_dict_to_get'] = syms_dict_to_get
    ld_dict['syms_data'] = syms_dict
    ld_dict['syms_fpaths'] = local_stock_data
    return ld_dict

# %% codecell
###############################################################################


class RecordHolidays():
    """Record holidays from IEX reference data."""
    base_path = baseDir().path
    fpath, df = '', None

    def __init__(self):
        self.construct_fpaths(self)
        self.check_existing(self)
        if not isinstance(self.df, pd.DataFrame):
            next_url, last_url, payload = self.construct_params(self)
            self.get_data(self, next_url, last_url, payload)
            self.write_to_json(self)

    @classmethod
    def construct_fpaths(cls, self):
        """Construct fpaths to use for data storage."""
        self.fpath = f"{self.base_path}/ref_data/holidays.gz"

    @classmethod
    def check_existing(cls, self):
        """Check for existing dataframe."""
        if os.path.isfile(self.fpath):
            self.df = pd.read_json(self.fpath, compression='gzip')

    @classmethod
    def construct_params(cls, self, type='holiday', last=50):
        """Construct parameters for get request."""
        load_dotenv()
        base_url = os.environ.get("base_url")
        payload = {'token': os.environ.get("iex_publish_api")}

        next_url = f"{base_url}/ref-data/us/dates/{type}/next/{last}"
        last_url = f"{base_url}/ref-data/us/dates/{type}/last/{last}"

        return next_url, last_url, payload

    @classmethod
    def get_data(cls, self, next_url, last_url, payload):
        """Get all holidays and store within class df."""
        next_df = pd.DataFrame(requests.get(next_url, params=payload).json())
        last_df = pd.DataFrame(requests.get(last_url, params=payload).json())

        all_df = pd.concat([next_df, last_df]).reset_index(drop=True)
        self.df = all_df

    @classmethod
    def write_to_json(cls, self):
        """Write data to local compressed json."""
        self.df.to_json(self.fpath)

# %% codecell
###############################################################################
