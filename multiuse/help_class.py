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
import inspect
import glob
import os
import json
from pathlib import Path
from datetime import timedelta, date
import datetime
import pytz
import time
from gzip import BadGzipFile
import zoneinfo
from zoneinfo import ZoneInfo

import pandas as pd
from pandas.tseries.offsets import BusinessDay
from pandas.tseries.offsets import CustomBusinessDay
from pandas.api.types import infer_dtype
import numpy as np
import dask.dataframe as dd
import requests
from dateutil.parser import parse
from pyarrow.lib import ArrowInvalid

# %% codecell
###############################################################################


def help_print_arg(arg, isp=False):
    """Print arg on local or server side."""
    if not isinstance(arg, str):
        arg = str(arg)

    if isp:
        if isp[1][3] != '<module>':
            arg = f"{isp[1][3]} {isp[0][3]} {arg}"
        else:
            arg = f"{isp[0][3]} {arg}"

    try:
        from app.tasks_test import print_arg_test
        print_arg_test.delay(arg)
    except ModuleNotFoundError:
        print(arg)
    except TypeError:
        pass


def check_nan(a, b=np.NaN):
    """Check nans."""
    try:
        if a.dtype == 'O':
            if a.str.contains('NaN'):
                a = a.astype(np.float64)
    except AttributeError:
        pass

    return (a == b) | ((a != a) & (b != b))


def round_cols(df, cols=False, decimals=3):
    """Round specified columns, or round all columns."""

    if not cols:
        cols = (df.select_dtypes(include=[np.float32, np.float64])
                  .columns.tolist())

    df[cols] = df[cols].astype(np.float64)
    df[cols] = df[cols].round(decimals)

    return df


def write_to_pickle(df, fpath, combine=False):
    """Write dataframe to picklel with optional combine param."""
    df = dataTypes(df).df

    if not isinstance(fpath, Path):
        fpath = Path(fpath)
    if 'pkl' not in str(fpath) and 'pickle' not in str(fpath):
        help_print_arg(f"write_to_pickle: {str(fpath)} not a pickle file")
        raise OSError
    # If parent directory doesn't exist, make it
    if not fpath.parent.exists():
        fpath.parent.mkdir(mode=0o777, parents=True)
        fpath.parent.chmod(mode=0o777)

    if combine:
        if fpath.exists():
            df_old = pd.read_pickle(fpath)
            df = pd.concat([df_old, df])

    df.to_pickle(fpath)


def write_to_parquet(df, fpath, combine=False, drop_duplicates=False, **kwargs):
    """Writing to parquet with error exceptions."""
    verbose = kwargs.get('verbose', False)
    cols_to_cat = kwargs.get('cols_to_cat', False)
    cols = df.columns
    for col in cols:  # If datatype is mixed. Convert to str
        try:
            if infer_dtype(col) in ['mixed', 'mixed-integer']:
                df[col] = df[col].astype('str')
            elif isinstance(df[col], pd.Timestamp):
                df[col] = df[col].apply(lambda x: x.value)
            elif infer_dtype(col) in 'datetime64':
                df[col] = df[col].apply(lambda x: x.value)
        except TypeError:
            pass

    df = dataTypes(df, parquet=True).df
    fpath = Path(fpath)
    # If parent directory doesn't exist, make it
    if not fpath.parent.exists():
        fpath.parent.mkdir(mode=0o777, parents=True)
        fpath.parent.chmod(mode=0o777)

    # Combine with existing dataframe if combine=True
    if combine & fpath.exists():
        df_old = pd.read_parquet(fpath)
        try:
            if cols_to_cat:  # Convert cols to categorical if they exists
                cols_to_cat = df_old.columns.intersection(cols_to_cat)
                df_old[cols_to_cat] = df_old[cols_to_cat]
        except Exception as e:
            if verbose:
                help_print_arg(f"""write_to_parquet cols_to_cat fail with
                               cols {str(cols_to_cat)}""")

        df = pd.concat([df_old, df]).copy()

        if drop_duplicates or 'cols_to_drop' in kwargs.keys() or 'cols_subset' in kwargs.keys():
            if 'cols_to_drop' in kwargs.keys():
                cols = kwargs['cols_to_drop']
                df.drop_duplicates(subset=cols, inplace=True)
            elif 'cols_subset' in kwargs.keys():
                cols = kwargs['cols_subset']
                df.drop_duplicates(subset=cols, inplace=True)
            else:
                df.drop_duplicates(inplace=True)
        if df.index.is_numeric():
            df = df.reset_index(drop=True)

    try:
        df.to_parquet(fpath, allow_truncated_timestamps=True)
    except (FileNotFoundError, OSError):
        if not fpath.parent.exists():
            fpath.parent.mkdir(mode=0o777, parents=True)
        write_to_parquet(df, fpath)
    except TypeError:  # For dask use cases
        df.to_parquet(fpath)
    except ArrowInvalid as ae:  # Then write to pkl
        help_print_arg(f"write_to_parquet ArrowInvalid: {type(ae)} {str(ae)}")
        fpath_pkl = fpath.parent.joinpath(f"{fpath.stem}.pickle")
        if fpath_pkl.exists():
            df_pk = pd.read_pickle(fpath_pkl)
            df = pd.concat([df, df_pk])
        df.to_pickle(fpath_pkl)
    except ValueError as ve:
        help_print_arg(f"Could not convert {str(fpath)} with reason {str(ve)}")
        # If problem validating dataframe, write dataframe to gz
        # Needed a fallback so data doesn't get lost
        fnew = Path(f"{str(fpath)[:-8]}_not_converted.gz")
        df.to_json(fnew, compression='gzip')


def help_print_error(e, parent=False, other=False, resp=False, ud=False):
    """Output helpful information from error."""

    """
    e      : Error : Exception as e
    parent : Parent function : string
    other  : Other useful params : string
    resp   : Url response : Response object
    ud     : urlData class : Class with .get
    """

    # 1st message
    if parent and other:
        help_print_arg(f"{parent} function error for {other}")
    elif parent and not other:
        help_print_arg(f"{parent} function error")

    # 2nd message
    help_print_arg(f" with error type: {type(e)} and error: {str(e)}")

    # 3rd message
    if resp:
        msg_1 = f"Url: {resp.url} get.status_code: {resp.status_code} "
        msg_2 = f"message: {resp.text}"
        help_print_arg(f"{msg_1}{msg_2}")
    elif ud:
        msg_1 = f"Url: {ud.get.url} get.status_code: {ud.get.status_code} "
        msg_2 = f"message: {ud.get.text}"
        help_print_arg(f"{msg_1}{msg_2}")


def df_create_bins(df, bin_size=1000):
    """Create bins sizes of default 1000 each. Add to df."""
    if isinstance(df.index, object):
        df.reset_index(drop=True, inplace=True)

    qcut_bin_n = df.shape[0] // bin_size
    bin_labels = list(range(1, qcut_bin_n + 1))

    # Create column with bin numbers
    df['bins'] = pd.qcut(df.index.tolist(), q=qcut_bin_n, labels=bin_labels)
    # Return dataframe
    return df


def check_size(var, name=False):
    """Check size of variable in megabytes."""
    from sys import getsizeof
    mb = round(getsizeof(var) / 1000000, 2)

    if name:
        help_print_arg(f"Var {name} is {mb} mb")
    else:
        help_print_arg(f"Var is {mb} mb")


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
    def daterange(days=4, *args, **kwargs):
        # dt_prev, dt
        dt = kwargs.get('dt', getDate.query('iex_close'))
        dt_prev = kwargs.get('date_prev', (dt - timedelta(days=days)))
        for n in range(int((dt - dt_prev).days)+1):
            yield dt_prev + timedelta(n)

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
    def date_to_rfc(dt):
        """Convert datetime.date to RFC-3339 format."""
        from datetime import datetime

        tz = pytz.timezone('US/Eastern')
        dt_now = datetime.now().min.time()
        dt_comb = datetime.combine(dt, dt_now)
        dt_tz_aware = dt_comb.replace(tzinfo=tz)
        dt_rfc = dt_tz_aware.isoformat()
        return dt_rfc

    @staticmethod
    def rfc_to_date(dt):
        """Convert rfc 3339 to datetime.date."""
        dt_return = ''
        if isinstance(dt, pd.Series):
            dt_return = pd.to_datetime(dt).dt.date
        else:
            dt_return = parse(dt).date()

        return dt_return

    @staticmethod
    def get_hol_list(this_year=True):
        """Get holiday list."""
        holidays_fpath = Path(baseDir().path, 'ref_data/holidays.parquet')
        holidays = pd.read_parquet(holidays_fpath)
        dt = getDate.query('sec_master')
        current_holidays = None

        if this_year:
            current_holidays = (holidays[(holidays['date'].dt.year >= dt.year) &
                                         (holidays['date'].dt.date <= dt)])
        else:
            current_holidays = holidays[holidays['date'].dt.date <= dt]

        hol_list = current_holidays['date'].dt.date.tolist()
        return hol_list

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
        weekend, query_date = False, False
        if date.today().weekday() in (5, 6):
            weekend = True

        # df_cal = ApcaPaths(api_val='calendar', return_df=True).df
        bd1, bd2 = BusinessDay(n=1), BusinessDay(n=2)
        fpath = Path(baseDir().path, 'alpaca', 'calendar', '_market_days.parquet')
        if fpath.exists():
            df_cal = pd.read_parquet(fpath)
            bd1 = CustomBusinessDay(calendar=df_cal['date'], n=1)
            bd2 = CustomBusinessDay(calendar=df_cal['date'], n=2)

        if site in ('cboe', 'occ'):
            if getDate.time_cutoff(cutoff_hm=16.15) or weekend:
                query_date = (date.today() - bd1).date()
        elif site in ('sec_rss'):  # 6 am start
            if getDate.time_cutoff(cutoff_hm=6.0) or weekend:
                query_date = (date.today() - bd1).date()
        elif site in ('iex_close'):
            if weekend:
                query_date = (date.today() - bd1).date()
        elif site in ('mkt_open'):
            if getDate.time_cutoff(cutoff_hm=9.0) or weekend:
                query_date = (date.today() - bd1).date()
        elif site in ('iex_eod'):
            if getDate.time_cutoff(cutoff_hm=16.15) or weekend:
                query_date = (date.today() - bd1).date()
        elif site in ('sec_master'):
            if getDate.time_cutoff(cutoff_hm=22.35) or weekend:
                query_date = (date.today() - bd1).date()
        elif site in ('iex_previous'):
            if getDate.time_cutoff(cutoff_hm=5.50) or weekend:
                query_date = (date.today() - bd2).date()
            else:
                query_date = (date.today() - bd1).date()
        elif site in ('last_syms'):
            pass

        # If none of the prev time/weekend conditions apply
        if not query_date:
            query_date = (date.today() - BusinessDay(n=0)).date()

        return query_date

    @staticmethod
    def apca_bus_days(year=None):
        """Get Alpaca calendar of business days."""
        bpath = Path(baseDir().path, 'alpaca', 'calendar')
        fpath = bpath.joinpath('_market_days.parquet')
        df_days = pd.read_parquet(fpath)

        # Add column for next date
        df_days['next_date'] = df_days['date'].shift(-1)

        if year:
            df_days = df_days[df_days['date'].dt.year == year].copy()

        if df_days.empty:
            print(f"""getDate.apca_bus_days - empty dataframe.
                   Year passed: {str(year)}""")

        return df_days

    @staticmethod
    def get_bus_day_diff(df, col1, col2):
        """Get the business day difference betweeen 2 columns."""
        holidays_fpath = Path(baseDir().path, 'ref_data/holidays.parquet')
        holidays = pd.read_parquet(holidays_fpath)
        dt = getDate.query('sec_master')
        current_holidays = (holidays[(holidays['date'].dt.year >= dt.year) &
                                     (holidays['date'].dt.date <= dt)])
        hol_list = current_holidays['date'].dt.date.tolist()
        dt_list = (df.apply(lambda row:
                   np.busday_count(row[col1].date(),
                                   row[col2].date(),
                                   holidays=hol_list),
                   axis=1
                  ))
        return dt_list.values

    @staticmethod
    def tz_aware_dt_now(offset=None, iso=False, rfcc=False, utc=False):
        """Get timezone aware datetime.now object."""
        from datetime import datetime
        tzinfo = ZoneInfo('US/Eastern')
        if utc:
            tzinfo = ZoneInfo('UTC')
        # zoneinfo.available_timezones()
        dt = datetime.now(tzinfo)
        if offset:  # Offset is measured in seconds
            dt = dt - timedelta(seconds=offset)
        if iso:
            dt = dt.isoformat('T')
        if rfcc:
            dt = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        return dt

    @staticmethod
    def get_bus_days(testing=False, this_year=False, cutoff=None, start_dt=None):
        """Get all business days. If file, read and return."""
        df, dt_year, fpath = None, None, ''
        if this_year:
            dt_year = getDate.query('iex_eod').year
            fpath = f"{baseDir().path}/ref_data/bus_days_{dt_year}.parquet"
        else:
            fpath = f"{baseDir().path}/ref_data/bus_days.parquet"

        if testing:
            print(fpath)

        if not os.path.isfile(fpath) or cutoff or this_year:
            rh = RecordHolidays().df['date']
            # Record the earliest/latest holiday and use that as range
            dt_min = rh.min().date()
            dt_max = rh.max().date()
            # Convert datetime index to series
            days_df = (pd.DataFrame(pd.bdate_range(dt_min, dt_max),
                                    columns=['date']))
            # Get all business days that are not holidays
            days = days_df[~days_df.isin(rh.tolist())]

            if this_year:
                days = days[days['date'].dt.year == dt_year].copy(deep=True)
            if cutoff:
                days = days[days['date'].dt.date <= cutoff].copy(deep=True)
            if start_dt:
                days = days[days['date'].dt.date >= start_dt].copy(deep=True)

            days.reset_index(drop=True, inplace=True)
            # Write to local json file
            # days.to_json(fpath, compression='gzip')
            days = dataTypes(days, parquet=True).df
            days.to_parquet(fpath)

            df = days.copy(deep=True)
        else:
            df = pd.read_parquet(fpath)

        return df

# %% codecell
###############################################################################


class dataTypes():
    """Helper class for implementing data type conversions."""
    explicit, parquet, df = False, False, False
    not_float_cols, not_float_cols_nans = [], []

    def __init__(self, df, resolve_floats=False, explicit=False, parquet=False):
        if isinstance(df, pd.DataFrame):
            self.dtypes, self.explicit, self.parquet = df.dtypes, explicit, parquet
            self.df = df.copy(deep=True)

            if resolve_floats:
                self.resolve_floats(self)
            if self.not_float_cols_nans:
                self.coerce_not_float_cols_nans(self)
            self._cols_to_cat(self)
            self.pos_or_neg_ints(self)
            self.pos_or_neg_floats(self)
            # print('Modified dataframe accessible with xxx.df')
        else:
            self.df = df

    @classmethod
    def resolve_floats(cls, self):
        """Test all float columns for integer coercion instead."""
        not_float_cols, not_float_cols_nans = [], []
        col_floats = self.df.select_dtypes(include=[np.float]).columns.tolist()
        for col in col_floats:
            if self.df[col].isna().sum():  # If there are any nans
                # But all remaining rows should be integers and not floats
                if all(n.is_integer() for n in self.df[col].dropna()):
                    not_float_cols_nans.append(col)
            # If there aren't any nans and the float col should be an integer
            elif all(n.is_integer() for n in self.df[col]):
                not_float_cols.append(col)

        self.not_float_cols = not_float_cols
        self.not_float_cols_nans = not_float_cols_nans

        if self.explicit:
            print(not_float_cols)
            print(f"Not float cols with nans: {not_float_cols_nans}")

    @classmethod
    def coerce_not_float_cols_nans(cls, self):
        """Coerce cols with floats and nans to the correct integer dtype."""
        cols = self.not_float_cols_nans

        int8_val = 127
        int16_val = 32767
        int32_val = 2147483648

        for col in cols:
            min = self.df[col].min()
            max = self.df[col].max()
            if min >= 0:
                if max < 255:
                    self.df[col] = self.df[col].astype(pd.UInt8Dtype())
                elif max < 65535:
                    self.df[col] = self.df[col].astype(pd.UInt16Dtype())
                elif max < 4294967295:
                    self.df[col] = self.df[col].astype(pd.UInt32Dtype())
            else:
                if min > -int8_val and max < int8_val:
                    self.df[col] = self.df[col].astype(pd.Int8Dtype())
                elif min > -int16_val and max < int16_val:
                    self.df[col] = self.df[col].astype(pd.Int16Dtype())
                elif min > -int32_val and max < int32_val:
                    self.df[col] = self.df[col].astype(pd.Int32Dtype())

    @classmethod
    def _cols_to_cat(cls, self):
        """Convert object columns to categorical."""
        cols_to_cat = self.dtypes[self.dtypes == 'object'].index.to_list()
        date_cols = []
        for col in cols_to_cat:
            if 'date' in str(col):
                date_cols.append(str(col))
            elif 'time' in str(col):
                date_cols.append(str(col))
        # Only keep columns that aren't in date col list
        cols_to_cat = [col for col in cols_to_cat if str(col) not in date_cols]

        try:
            self.df[cols_to_cat] = self.df[cols_to_cat].astype('category')
        except TypeError:
            for col in cols_to_cat:
                try:
                    self.df[col] = self.df[col].astype('category')
                except Exception as e:
                    if self.explicit:
                        help_print_arg(f"Object column {col} with error: {str(e)}")

        self.df[date_cols] = self.df[date_cols].astype('str')

    @classmethod
    def pos_or_neg_ints(cls, self):
        """Convert integers to correct data type."""
        cols_int = self.df.select_dtypes(include=[np.int]).columns.tolist()
        # Check if there are
        if self.not_float_cols:
            cols_int = cols_int + self.not_float_cols

        int8_val = 127
        int16_val = 32767
        int32_val = 2147483648

        for col in cols_int:
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
                if min > -int8_val and max < int8_val:
                    self.df[col] = self.df[col].astype(np.int8)
                elif min > -int16_val and max < int16_val:
                    self.df[col] = self.df[col].astype(np.int16)
                elif min > -int32_val and max < int32_val:
                    self.df[col] = self.df[col].astype(np.int32)

    @classmethod
    def pos_or_neg_floats(cls, self):
        """Convert floats to correct data type."""
        float_type = [np.float, 'float16', 'float32', 'float64']
        cols_float = self.df.select_dtypes(include=float_type).columns.tolist()

        for col in cols_float:
            _min = self.df[col].min()
            _max = self.df[col].max()
            if _min > np.finfo('float16').min and _max < np.finfo('float16').max:
                if self.parquet:
                    self.df[col] = self.df[col].astype(np.float32)
                else:
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
            syms_dict[st] = pd.read_parquet(path)
        except BadGzipFile:
            syms_dict[st] = pd.read_json(path)
        except:
            error = True
        finally:
            if error:
                os.remove(path)

        # syms_dict[st]['date'] = pd.to_datetime(syms_dict[st]['date'], unit='ms')
        try:
            syms_dict_to_get[st] = dl_ser[~dl_ser_dt.isin(
                syms_dict[st]['date'].astype('object'))]
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
            self.write_to_parquet(self)

    @classmethod
    def construct_fpaths(cls, self):
        """Construct fpaths to use for data storage."""
        self.fpath = f"{self.base_path}/ref_data/holidays.parquet"

    @classmethod
    def check_existing(cls, self):
        """Check for existing dataframe."""
        if os.path.isfile(self.fpath):
            self.df = pd.read_parquet(self.fpath)

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
        all_df = dataTypes(all_df, parquet=True).df
        self.df = all_df

    @classmethod
    def write_to_parquet(cls, self):
        """Write data to local compressed json."""
        self.df.to_parquet(self.fpath)

# %% codecell
###############################################################################
