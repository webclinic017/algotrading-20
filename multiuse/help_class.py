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
from pathlib import Path
from datetime import timedelta, date
import datetime
import pytz
from pandas.tseries.offsets import BusinessDay
from gzip import BadGzipFile

import pandas as pd
import numpy as np
# %% codecell
###############################################################################

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
        query_date = ''
        if site in ('cboe', 'occ'):
            if getDate.time_cutoff(cutoff_hm=17.15):
                query_date = (date.today() - BusinessDay(n=1)).date()
            else:
                query_date = (date.today() - BusinessDay(n=0)).date()
        elif site in ('iex_close'):
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

# %% codecell
###############################################################################

class dataTypes():
    """Helper class for implementing data type conversions."""

    def __init__(self, df):
        self.dtypes = df.dtypes
        self.df = df.copy(deep=True)
        self._cols_to_cat(self)
        self.pos_or_neg_ints(self)
        self.pos_or_neg_floats(self)
        print('Modified dataframe accessible with xxx.df')

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
        self.df[cols_float64] = self.df[cols_float64].astype(np.float16)



# %% codecell
###############################################################################


def local_dates(which):
    """Get dict of missing local dates."""
    stock_suf = ({
        'StockEOD': f"/StockEOD/{date.today().year}/*/**"
    })

    stock_fpath = f"{baseDir().path}{stock_suf[which]}"
    # Min date for stocktwits trending data
    st_date_min = datetime.date(2021, 2, 19)
    dl_ser = pd.Series(getDate.busDays(st_date_min))

    # Get fpaths for all local syms
    local_stock_data = glob.glob(stock_fpath)
    local_stock_data = sorted(local_stock_data)

    local_syms = []  # Create an empty list
    for st in local_stock_data:  # Split strings and store symbol names
        local_syms.append(st.split('_')[1][:-3])

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
