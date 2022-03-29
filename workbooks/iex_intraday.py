"""IEX Intraday."""
# %% codecell
from pathlib import Path
import os
import sys
from datetime import time

import dask.dataframe as dd
from dask import compute

import pandas as pd
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv
import yfinance as yf
import requests
from pyarrow.lib import ArrowInvalid

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg, df_create_bins
    from scripts.dev.multiuse.sys_utils import get_memory_usage
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg, df_create_bins
    from multiuse.sys_utils import get_memory_usage
    from data_collect.iex_class import urlData
    from api import serverAPI

from importlib import reload
import sys

from data_collect.iex_intraday_resample import ResampleIntraday

reload(sys.modules['api'])
reload(sys.modules['multiuse.help_class'])
reload(sys.modules['data_collect.iex_class'])
reload(sys.modules['data_collect.iex_intraday_resample'])
# %% codecell
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 50)

# %% codecell
dt = getDate.query('iex_eod')
bpath = Path(baseDir().path, 'intraday', 'minute_1', str(dt.year))

all_syms = serverAPI('all_symbols').df
syms = all_syms['symbol'].unique()

# %% codecell
prev_dt = getDate.query('iex_previous')
prev_bpath = Path(baseDir().path, 'StockEOD')
prev_comb = prev_bpath.joinpath('combined', f"_{prev_dt}.parquet")

prev_comb
# serverAPI('redo', val='split_iex_hist_previous').df


# serverAPI('redo', val='combine_iex_intraday')

# %% codecell
# I'll have to wait for the minute_1 data to download.

serverAPI('redo', val='CombineIntradayQuotes')


# I'll have to come up with an interesting naming situation
# How about getting the most recent, then combining

# %% codecell

# This should really all be a class, it's  already complicated enough as it is
# Can add a current year option as well if necessary

# %% codecell
from datetime import date
dt = date(2021, 1, 1)
global_dict = {}

# %% codecell

ciq = CombineIntradayQuotes(**{'current': True, 'verbose': True})

# %% codecell

url = 'https://algotrading.ventures/api/v1/data/errors/clean_iex_1min'
get = requests.get(url)
from io import BytesIO


df_errs = pd.read_pickle(BytesIO(get.content))
df_errs['symbol'] = (df_errs['path'].str.split('_')
                     .str[-1].str.split('.').str[0])

df_errs['symbol'].values

errs = serverAPI('errors_iex_intraday_1min').df


# %% codecell

class CombineIntradayQuotes():
    """Combine intraday stock quotes."""

    cols_to_keep = (['symbol', 'dtime', 'date', 'marketHigh', 'marketLow',
                     'marketAverage', 'marketVolume', 'marketNotional',
                     'marketNumberOfTrades', 'marketOpen',
                     'marketClose', 'marketChangeOverTime'])

    def __init__(self, **kwargs):
        dirs = self._get_minute_dirs(self)
        self._start_minute_loop(self, dirs, **kwargs)

    @classmethod
    def _get_minute_dirs(cls, self):
        """Get minute_x directories."""
        bpath = Path(baseDir().path, 'intraday')
        # Only get directories with minute
        dirs = list(bpath.glob('minute*'))
        return dirs

    @classmethod
    def _start_minute_loop(cls, self, dirs, **kwargs):
        """Start for loop for minute directories."""
        for mdir in dirs:  # For each minute directory
            self._get_minute_paths(self, mdir, **kwargs)

    @classmethod
    def _get_minute_paths(cls, self, mdir, **kwargs):
        """Get all minute paths that fit criteria."""
        bpath = Path(baseDir().path, 'intraday')
        dt = kwargs.get('dt', None)
        if not dt:
            dt = getDate.query('iex_eod')
        yr = str(dt.year)

        f_cb = bpath.joinpath('combined', mdir.stem, yr, f"_{dt}.parquet")
        f_cb_all = Path(str(f_cb).replace('combined', 'combined_all'))

        current = kwargs.get('current', None)
        verbose = kwargs.get('verbose', None)
        testing = kwargs.get('testing', None)
        kwargs['f_cb'] = f_cb
        kwargs['f_cb_all'] = f_cb_all


        if current:  # Assume that we only want the current year
            # Get only the directories with letters of alphabet
            alph_dirs = mdir.joinpath(yr).glob('[!.]')
            for alph_dir in tqdm(alph_dirs):
                if verbose:
                    help_print_arg(f"{str(f_cb.parent.parent.stem)} {str(alph_dir)}")
                try:
                    self._combine_minute_files(self, alph_dir, **kwargs)
                except Exception as e:
                    print(f"{str(e)} {type(e)}")
                    if testing:
                        break
                if get_memory_usage():
                    time.sleep(20)

    @classmethod
    def _combine_minute_files(cls, self, alph_dir, **kwargs):
        """Concat paths, include exceptions for dask."""
        alph_paths = alph_dir.rglob('*.parquet')
        verbose = kwargs.get('verbose', False)

        # df_list = []
        cols_to_keep = self.cols_to_keep
        dd_all = dd.from_pandas(pd.DataFrame(), npartitions=1)
        for f in alph_paths:
            try:
                dd_new = dd.read_parquet(f, columns=self.cols_to_keep)
                dd_all = dd.concat([dd_all, dd_new])
                # df_list.append(dd.read_parquet(f, columns=cols_to_keep))
            except Exception as e:
                if verbose:
                    msg = f"fpath: {str(f)} reason: {str(e)}"
                    help_print_arg(msg)

        df_all = None
        try:
            if verbose:
                help_print_arg(f"Starting concat for {str(alph_dir)}")
            # dd_all = dd.concat(df_list)
            dd_all = dd_all.reset_index(drop=True)
            df_all = dd_all.compute().copy()
            if verbose:
                help_print_arg(f"Made it past compute for {str(alph_dir)}")
        except AttributeError as ae:
            help_print_arg(f"Could not concat, convert to dataframe. Exiting with error {str(ae)}")
        if isinstance(df_all, pd.DataFrame):
            self._write_files(self, df_all, **kwargs)

    @classmethod
    def _write_files(cls, self, df_all, **kwargs):
        """Write files."""
        f_cb, f_cb_all = Path(kwargs['f_cb']), Path(kwargs['f_cb_all'])
        verbose = kwargs.get('verbose', False)

        # f_all_prev = get_most_recent_fpath(Path(f_cb_all).parent)
        # if not f_all_prev:
        #     f_all_prev = f_cb_all
        # if verbose:
        #    help_print_arg(f"Writing f_cb_all {str(f_all_prev)}")
        write_to_parquet(df_all, f_cb_all, combine=True)

# %% codecell


# %% codecell


# %% codecell


# %% codecell



# %% codecell


# %% codecell


# %% codecell

# %% code


# %% codecell
from multiuse.path_helpers import get_most_recent_fpath


f_all_prev = get_most_recent_fpath(Path(f_cb).parent)
write_to_parquet(df_all, f_all_prev, combine=True)
f_all_prev.rename(f_cb_all)

# %% codecell

df_cb = df_all[df_all['date'] == df_all['date'].max()]


# %% codecell
f = Path(baseDir().path, 'intraday', 'minute_1', '2022')
fpath_list = list(f.glob('**/*.parquet'))

intra = serverAPI('iex_intraday_m1').df

df_list = combine_all_intraday_data()

# %% codecell




# %% codecell


# %% codecell



# %% codecell



dd_all = dd_all.dropna(subset=['marketHigh', 'marketLow'])


df_all = dd_all.compute()


dd_test = dd.from_pandas(df_all, npartitions=1)

cat_cols = dd_test.select_dtypes(include='category')

dd_test[cat_cols] = dd_test[cat_cols].astype('category')

dir(dd_test.select_dtypes(include='category'))

dd_known = dd_test.categorize()
dd_known.col.cat.known
.cat.known
dd_test.select_dtypes('category')
type(dd_test)


f = Path('/Users/eddyt/Algo/data/intraday/minute_5/2022/w')
path_cb_all = '/Users/eddyt/Algo/data/intraday/combined_all/minute_5/2022/_2022-02-08.parquet'
df_all = write_combined_intraday_data(f, path_cb_all)
dd_all = dd.from_pandas(df_all, npartitions=1)

# %% codecell

# %% codecell

# serverAPI('redo', val='iex_intraday_all')

bpath = Path(baseDir().path, 'ml_data/fib_analysis')
fib_all_path = bpath.joinpath('fib_all_cleaned_data.parquet')

df_fib = pd.read_parquet(fib_all_path)
df_fib[df_fib['symbol'] == 'NIO']

# %% codecell

serverAPI('redo', val='resample_iex_intraday')
serverAPI('redo', val='combine_iex_intraday')
# serverAPI('redo', val='add_sym_col_to_intraday')
# %% codecell

df_m1 = serverAPI('iex_intraday_m1').df


serverAPI('redo', val='combine_iex_minute_3')
# %% codecell


# %% codecell

# Can't get premarket data from IEX

# %% codecell

sym = 'A'
df_yf = yf.download(sym, interval='1m', period='7d', prepost=True)
cols_to_rename = {col: f"market{col.replace(' ', '')}" for col in df_yf.columns}
df_yf.rename(columns=cols_to_rename, inplace=True)
df_yf.reset_index(inplace=True)

df_yf.insert(0, 'symbol', sym)
df_yf.insert(1, 'year', df_yf['Datetime'].dt.year)
df_yf.insert(2, 'date', pd.to_datetime(df_yf['Datetime'].dt.date))
df_yf.insert(3, 'minute', df_yf['Datetime'].dt.minute)

df_yf['symbol'] = sym
df_yf['minute'] = df_yf['Datetime'].dt.minute

dtest = df_yf.copy()
dtest.rename(columns={'Datetime': 'dtime'}, inplace=True)
dtest['minute'] = dtest['dtime'].dt.time

dtest['source'] = 'yf'

dtest_a = dtest[dtest['date'] == '2022-01-20'].copy()
dtest_a['dtime'] = dtest_a['dtime'].dt.tz_localize(None)

fpath = '/Users/eddyt/Algo/data/intraday/minute_1/2022/a/_A.parquet'
df_a = pd.read_parquet(fpath)
df_a['source'] = 'iex'


dtest_all = pd.concat([df_a, dtest_a])

# %% codecell

dtest_all['mHours'] = (np.where((
    (dtest_all['dtime'].dt.time >= time(hour=9, minute=30))
    & (dtest_all['dtime'].dt.time <= time(hour=16, minute=0))
), 1, 0))

bh_test = dtest_all[dtest_all['mHours'] == 1]
non_bh_test = dtest_all[dtest_all['mHours'] != 1]

bh_test.drop_duplicates(subset=['dtime'], inplace=True)

df_all = (pd.concat([bh_test, non_bh_test])
            .sort_values(by=['dtime'], ascending=True))
# %% codecell

df_all
# %% codecell
# serverAPI('redo', val='iex_intraday_last_30')

# %% codecell

# %% codecell

df_m1

# %% codecell

df_m1['marketVolume'].isna().sum()
df_m2 = df_m1.dropna(subset=['marketVolume'])

df_m2['symbol'].isna().sum()
df_m2.columns

# %% codecell
df_m1 = serverAPI('iex_intraday_m1').df

# path = Path(baseDir().path, 'intraday', 'minute_1')


# %% codecell
df_m2 = df_m1.dropna(subset=['marketHigh', 'marketLow'])

cols_to_keep = (['symbol', 'dtime', 'marketHigh', 'marketLow',
                 'marketAverage', 'marketVolume',
                 'marketNotional', 'marketNumberOfTrades', 'marketOpen',
                 'marketClose', 'marketChangeOverTime'])

df_m2.columns


df_m2

df_m1

# %% codecell

















# %% codecell
