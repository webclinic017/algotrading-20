"""Analyzing cboe symbol options data."""
# %% codecell
from pathlib import Path
from datetime import datetime, date, timedelta

import requests
import pandas as pd
import numpy as np

from io import BytesIO

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
    from scripts.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.data_collect.cboe_intraday import CboeIntraday
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions
    from data_collect.cboe_intraday import CboeIntraday
    from api import serverAPI

# %% codecell
from missing_data.get_missing_hist_from_yf import get_yf_loop_missing_hist

sym_list = ['GENI']
get_yf_loop_missing_hist(sym_list=sym_list)
# %% codecell
# I'd like 2 dataframes:
# One with the data updated every 10 minutes that I can
# clean later on
# Another with the raw timestamps that
# I'll have to sort through later on
from multiuse.path_helpers import get_most_recent_fpath

dt = getDate.query('iex_close')
bpath = Path(baseDir().path, 'derivatives/cboe_intraday/2021')
fpath = get_most_recent_fpath(bpath, f_suf='_eod', dt=dt)

# %% codecell
from workbooks.fib_funcs import read_clean_combined_all
df_all = read_clean_combined_all()

# Eagles golf club

# path_eod won't have the timestamps
# %% codecell


# %% codecell
sapi_eod = serverAPI('cboe_intraday_eod')
df_cboe = sapi_eod.df
cols_to_rename = ({'Symbol': 'symbol', 'Call/Put': 'side',
                   'Expiration': 'expirationDate',
                   'Strike Price': 'strike'})
df_cboe.rename(columns=cols_to_rename, inplace=True)
df_cboe['symbol'] = df_cboe['symbol'].astype('category')

# %% codecell
sapi = serverAPI('yoptions_daily')
df = sapi.df
# %% codecell
df_last = (df[df['lastTradeDate'].dt.date == df['lastTradeDate'].dt.date.max()]
           .reset_index().copy())
df_last.drop(columns=['side', 'strike', 'expDate'], inplace=True)

cboe_symref = serverAPI('cboe_symref').df
cboe_symref['OSI Symbol'] = cboe_symref['OSI Symbol'].str.replace(' ', '')
cboe_symref = cboe_symref[['OSI Symbol', 'side', 'strike', 'expirationDate']]
df_merged = (pd.merge(df_last, cboe_symref, left_on='contractSymbol',
                      right_on='OSI Symbol', how='left'))
df_merged.drop_duplicates(inplace=True)
df_merged['expirationDate'] = (pd.to_datetime(df_merged['expirationDate'],
                                              format='%y%m%d'))
df_merged['strike'] = df_merged['strike'].astype(np.float32)

df_syms = serverAPI('all_symbols').df
df_cs = df_syms[df_syms['type'].isin(['cs', 'ad'])]
cs_list = df_cs['symbol'].tolist()

on = ['symbol', 'side', 'expirationDate', 'strike']
df_comb = pd.merge(df_merged, df_cboe, on=on)
df_comb.insert(1, 'rout/vol', df_comb['Routed'].div(df_comb['Volume']).round(2))
(df_comb.insert(1, 'vol/oi', df_comb['Volume'].div(df_comb['openInterest']
        .replace(0, 1), fill_value=0)
        .round(2)))
# %% codecell


cols_to_drop = ['OSI Symbol', 'contractSymbol', 'lastTradeDate']
col_order = (['symbol', 'vol/oi', 'rout/vol', 'side', 'strike',
              'expirationDate', 'volume', 'Volume', 'openInterest',
              'lastPrice', 'inTheMoney', 'impliedVolatility'])
# Conditions
conds = (df_comb['symbol'].isin(cs_list)) & (df_comb['openInterest'] > 1)
df_comb.loc[conds].drop(columns=cols_to_drop)[col_order].sort_values(by=['vol/oi'], ascending=False).head(25)
df_comb.columns


# %% codecell


df_last.head()

# %% codecell

# From 9:31 until market close

# Cool, so there are 2,976 different unique symbols on a friday
# I wonder what the cutoff is for this. I can probably request every 10 minutes
# Then I'd have to subtract from the previous 10 minute dataframe,
# which is where this stuff gets complicated

# What I can do is combine with the previous days data for open interest, avg volume,
# Then analyze in real time without paying a fortune.

# Can also analyze based on routed, sign of unusual volume
# Exclude etfs

all_syms = serverAPI('all_symbols').df
all_cs = all_syms[all_syms['type'].isin(['cs', 'ad'])]

df_eq = df[df['symbol'].isin(all_cs['symbol'].tolist())].copy()

# This is where the iex stats comes in handy
sc_api = serverAPI('stats_combined')
df_stats = sc_api.df
# Temporary measure to deal with the lack of a date column
df_stats.drop_duplicates(subset=['companyName'], inplace=True)

# Okay so we'll drop duplicates based on the company name
df_stats_ref = pd.merge(all_syms, df_stats, left_on=['name'], right_on=['companyName'])
df_stats_ref['marketcap'] = (df_stats_ref['marketcap'].div(1000000, fill_value=0)).astype('int64')
df_stats_ref.drop(columns=['date'], inplace=True)

# Combine df_stats_ref with df_eq
df_eq = pd.merge(df_eq, df_stats_ref, on=['symbol'])

# If data is current
dt_close = getDate.query('iex_close')
if dt_close == date.today():
    df_eq['time'] = pd.Timestamp.now()

# %% codecell
df_eq.info()
df_stats_ref.columns
cols_to_keep = df.columns.tolist() + ['marketcap', 'float', 'beta', 'day5ChangePercent', 'nextEarningsDate']

# If on Friday, we could exclude that Friday as may not be unusual volume

df_eq.sort_values(by=['routed'], ascending=False)[cols_to_keep].head(50)


df.sort_values(by=['Routed'], ascending=False).head(50)



df['Symbol'].unique().shape


df.info()

# %% codecell
