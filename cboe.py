"""
CBOE Options Data
"""
# %% codecell
##############################################################
import os
import json
from json import JSONDecodeError
from io import StringIO, BytesIO
import glob
import importlib
import sys
import copy

import pandas as pd
from pandas.tseries.offsets import BusinessDay
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path
import base64

import datetime
from datetime import date, timedelta

from charset_normalizer import CharsetNormalizerMatch
from charset_normalizer import detect
from charset_normalizer import CharsetNormalizerMatches as CnM

import xml.etree.ElementTree as ET

from multiuse.help_class import baseDir, getDate
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import baseDir, getDate

from data_collect.iex_class import readData
importlib.reload(sys.modules['data_collect.iex_class'])
from data_collect.iex_class import readData, urlData

from data_collect.cboe_class import cboeData, cleanMmo, cboeLocalRecDiff
importlib.reload(sys.modules['data_collect.cboe_class'])
from data_collect.cboe_class import cboeData, cleanMmo, cboeLocalRecDiff

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 50)

# %% codecell
##############################################################



# %% codecell
##############################################################
# cboe_redo = serverAPI('redo', val='combine_cboe')

url = 'https://algotrading.ventures/api/v1/data/cboe/dump/all'
get = requests.get(url)
df = pd.DataFrame(get.json())

df['reportDate'] = pd.to_datetime(df['rptDate'].str[-13:-3], format='%Y-%m-%d')

cols_to_drop = [col for col in df.columns if 'strike' in col or 'rpt' in col]

df_sub = df.drop(columns=cols_to_drop).copy()
df_sub['expDate'] = pd.to_datetime(df_sub['expDate'], unit='ms')

# %% codecell

df_sym_sub = df_sub['Underlying']
cols_to_rename = {'reportDate': 'date', 'Underlying': 'symbol'}
df_sub.rename(columns=cols_to_rename, inplace=True)

# Read historical data collected from IEX
combined_fpath = Path(baseDir().path, 'historical', 'combined', 'sub.parquet')
df_hist = pd.read_parquet(combined_fpath)
# Only include values after 2020
df_hist = df_hist[df_hist.index.get_level_values('date') > '2020']
df_use = df_hist[df_hist.index.get_level_values('symbol').isin(df_sub['symbol'].tolist())].copy()
df_use['range'] = df_use['fHigh'] - df_use['fLow']



# Create percentage change columns for the following days
periods = [1, 2, 3, 5, 10]
periods_to_cols = [f"pc_{p}" for p in periods]
df_use.sort_index(level='symbol', inplace=True)
# Cycle through percentage change columns, round to 0 decimal places
for p, col in zip(periods, periods_to_cols):
    df_use[col] = (-df_use['fClose'].pct_change(periods=-p) * 100).round(0)


# df_use['diff_10'] = ((df_use['fClose'].shift(periods=-10) - df_use['fClose']) / df_use['fClose'])
# df_use['fClose'].pct_change(-10).head(10)
# df_use['diff_10'].head(10)

# %% codecell
# Reset index, merge with cboe data
df_use_res = df_use.reset_index()
df_use_res['date'] = pd.to_datetime(df_use_res['date'], format='%Y-%m-%d')

df_comb = pd.merge(df_use_res, df_sub, on=['symbol', 'date'])
# %% codecell

df_comb.info()

df_comb.head()

# %% codecell

fpath = '/Users/unknown1/Algo/data/derivatives/unusual/comb_hist.parquet'
df_comb = df_comb.reset_index(drop=True)
df_comb.to_parquet(fpath)
# df_comb.head(10)
# df_comb.groupby(by=['symbol', 'date', 'side']).sum().sort_values(by=['pc_3', 'vol/avg'], ascending=False).head(50)
# df_comb.sort_values(by=['pc_3', 'vol/avg'], ascending=False).head(50)

# %% codecell


# %% codecell
##############################################################
df_comb = pd.read_parquet(fpath)

df = df_comb.set_index(['date', 'symbol']).copy()
df.info()

df_comb.head()

# %% codecell
##############################################################
column_order = (['side', 'vol/avg', 'totVol', 'Cboe ADV', 'pc_1', 'pc_3',
                 'pc_5', 'expDate', 'fVolume', 'fClose', 'range', 'pc_2',
                 'pc_10', 'liq_opp', 'fOpen', 'fHigh', 'fLow', 'vol_opp'])
df = df[column_order].copy()
# df[(df['pc_10'] > 10) & (df['pc_3'].between(-2, 3, inclusive=True))].sort_values(by=['pc_10', 'vol/avg'], ascending=False).head(50)

# df.sort_values(by=['pc_10', 'vol/avg'], ascending=False).head(50)

df['side_pc10+'] = np.where((df['side'] == 'C') & (df['pc_10'] > 10), 1, 0)
df['side_pc10-'] = np.where((df['side'] == 'P') & (df['pc_10'] < -10), 1, 0)
df['side_pc5+'] = np.where((df['side'] == 'C') & (df['pc_5'] > 10), 1, 0)
df['side_pc5-'] = np.where((df['side'] == 'P') & (df['pc_5'] < -10), 1, 0)
df['days_diff'] = df['expDate'] - df.index.get_level_values('date')

# df[df.index.get_level_values('symbol') == 'ECHO']

df_short = df[df['days_diff'] < timedelta(days=31)]

df_short.sort_values(by=['side_pc10+', 'vol/avg'], ascending=False).head(50)

# (5,100) or 5/45 or 10% of signals are profitable using the pc_10

df_short.head()
(df_short.reset_index().drop_duplicates(subset=['symbol', 'vol/avg'])
         .sort_values(by=['side_pc10+', 'vol/avg'], ascending=False)
         .set_index('date', 'symbol').head(50))
# df.sort_values(by=['side_pc10+', 'vol/avg'], ascending=False).head(50)


# %% codecell
##############################################################


cboe_df['date_dt'] = pd.to_datetime(cboe_df['dataDate'])
cboe_df['date_df'] = (cboe_df['date_dt'] + bs).dt.date

my_watch = serverAPI('st_watch').df.T
my_syms =  my_watch['symbols'].values.tolist()


# %% codecell
##############################################################
# Threading

import logging
import threading
import time
import concurrent.futures

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(messae)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(thread_function, range(3))

# %% codecell


# %% codecell
##############################################################
top_df_og = cboeLocalRecDiff(which='top_2000', fresh=True).df
top_df = top_df_og.copy(deep=True)

# top_df_og[(top_df_og['Underlying'] == 'YETI') & (top_df_og['expDate'] == '2021-02-26')]
"""
dataDate = 2021-02-24
VOD:  Vol/avg = 260 - next day VOD tanks
CIEN  Vol/avg = 50 - drops less than the rest
EOG   Vol/avg = 30 - spikes up/drops
CMG hardly moved, but calls bought on green
YETI tanked despite calls bought on red
OKE calls bought on green, then tanked
KMB calls bought on slight red, then tanked
URI puts bought on green, then epically tanks
WBA calls bought on green, barely moved
SLB puts bought on big green, tanks right after
FL puts bought on big green, tanks right after
STZ goes up green, then tanks right after
NOW goes up green, tanks right after
MO goes up green, tanks right after
"""

top_df[(top_df['expDate'] == '2021-02-26') & (top_df['dataDate'] == '2021-02-24')].sort_values(by=['vol/avg', 'liq_opp'], ascending=False).head(50)

# %% codecell
##############################################################

# %% codecell
##############################################################



"""
Get the p/c volume ratio - not that helpful and only a rough approximation

pc_dict = {}
for dd in dataDates:
    this_date = thisF_df[thisF_df['dataDate'] == dd]
    group = this_date.groupby(by=['side']).sum().reset_index()
    pc_dict[f"{dd}_pc"] = group.iloc[0]['totVol'] / group.iloc[1]['totVol']
"""

"""
This is really interesting. Someone bought a much higher than normal number of puts on EOG as it hit its 52 week high.
THe next day, the same, and the day after that, and the day after that.

USB is the same. The stock is rising and yet people are buying puts. 2 days after, the stock dumps.
"""


# %% codecell
##############################################################

# Thesis to get a weekly volume, monthly volume. When weekly crosses over monthly
"""

base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/cboe"

cboe_base = "https://www.cboe.com/us/options/market_statistics/historical_data/download/"
cboe_u1 = "all_symbols/?reportType=volume&month=&year=2021&volumeType=sum&volumeAggType=daily"
cboe_u2 = "&exchanges=CBOE&exchanges=BATS&exchanges=C2&exchanges=EDGX"

cboe_url = f"{cboe_base}{cboe_u1}{cboe_u2}"
cboe_get = requests.get(cboe_url)

print(type(cboe_get.content))
print(len(cboe_get.content))

cboe_sample = cboe_get.content[0:10000]
cboe_sample

print(CnM.from_bytes(cboe_sample).best().first())
# %% codecell
##############################################################

cboe_df = pd.read_csv(
            BytesIO(cboe_get.content),
            escapechar='\n',
            delimiter=',',
            delim_whitespace=False,
            skipinitialspace=False
            )

cboe_df.shape

cboe_df['Product Type'].value_counts()

vol_sum_df = cboe_df.groupby(by=['Trade Date', 'Underlying']).sum()
# vol_sum_df.reset_index(inplace=True)
vol_sum_df['weeklyAvg'] = vol_sum_df['Volume'].rolling(window=5).sum()

vol_sum_df.head(100)


cboe_df.head(10)
"""

# %% codecell
##############################################################
"""
Missed Liquidity
    This is a measure for the last week of the average daily volume requested at a price equal or better than the NBBO where we had no liquidity.
Exhausted Liquidity
    This is a measure for the last week of the average daily volume requested at a price equal or better than the NBBO, which were partially filled.
Routed Liquidity
    This number represents for the last week the average daily volume on orders which were routed and filled on another venue.
Volume Opportunity
    This is a measure of the total average daily volume of the missed, exhausted and routed liquidity.
Cboe ADV
    Average Daily Volume for the last week of shares matched on Cboe for the security shown.
Liquidity Opportunity
    Percentage of the ADV missed, exhausted or routed. The higher the percentage the larger the market making opportunity.
"""

"""CBOE Options Exchanges
Cboe Options Exchange
    - Hybrid, Classic Model
    - Open outcry and electronic exchange
Cboe BZX Options Exchange
    - Price-Time, Maker-Taker Model
    - All-electronic exchange
Cboe C2 Options Exchange == 'ctwo'
    - Pro-Rata, Maker-Taker Model
    - All-electronic exchange
Cboe EDGX Options Exchange
    - Classic Pro-Rata/Customer Priority/DMM Model
    - All-electronic exchange

Writing covered called adds to the Open Interest

"""

# %% codecell
##############################################################

from pyinstrument import Profiler
profiler = Profiler()

from dev.cboe_class import cboeData, cleanMmo
importlib.reload(sys.modules['dev.cboe_class'])
from dev.cboe_class import cboeData, cleanMmo

from dev.iex_class import readData
from dev.help_class import dataTypes

# %% codecell
##############################################################

# The thesis is trying to find names that previously had liquidity and now don't
# From one week to another, this will give a good idea of where market makers are
# Forced to buy more options
"""
try:
    df = pd.merge(self.mmo_df, self.sym_df, on=['Symbol', 'exchange', 'Underlying'], how='left')
    df.reset_index(inplace=True, drop=True)
except TypeError:
    df = pd.DataFrame()
return df
"""
mmo = cboeData('mmo')

clean_mmo = cleanMmo(mmo)
top_df = clean_mmo.nopop_top_1000.copy(deep=True)
# %% codecell
##############################################################
clean_mmo.nopop_top_2000.sort_values(by='liq_opp', ascending=False).head(10)

# %% codecell
##############################################################

base_exp = f"{Path(os.getcwd()).parent}/data/derivatives/cboe/syms_to_explore"

for fpath in glob.glob(f"{base_exp}/*"):
    if 'short' in fpath:
        short_exp = pd.read_json(fpath, compression='gzip')
    elif 'medium' in fpath:
        medium_exp = pd.read_json(fpath, compression='gzip')
    elif 'long' in fpath:
        long_exp = pd.read_json(fpath, compression='gzip')

long_exp.sort_values(by='vol/avg', ascending=False).head(100)

st_syms = short_exp[short_exp['totVol'] > 200].sort_values(by=['vol/avg', 'liq_opp'], ascending=False).head(100)

st_syms.head(10)

# BSX showed an unusual number of puts traded on Feb 19th
# NRG - look at volume for the month
# KO - April 1st, $54 - 1617 compared to just 6 open interest
# %% codecell
##############################################################
"""

short_exp[short_exp['totVol'] > 200].sort_values(by=['vol/avg', 'liq_opp'], ascending=False).head(100)


kodk_long = top_df[['Underlying'] == 'KODK'].sort_values(by='expDate', ascending=True)
kodk_long.set_index(['expDate', 'side']).plot(kind='bar')

kodk_short = short_exp[short_exp['Underlying'] == 'KODK'].sort_values(by='expDate', ascending=True)
kodk_short.set_index(['expDate', 'side']).plot(kind='bar')

gps_short = short_exp[short_exp['Underlying'] == 'GPS'].sort_values(by='expDate', ascending=True)
gps_short.set_index(['expDate', 'side']).plot(kind='bar')


zm_short = short_exp[short_exp['Underlying'] == 'ZM']
zm_short.set_index(['expDate', 'side']).plot(kind='bar')

zm_short.plot.hist(by='expDate', x='expDate', y=['side', 'totVol'])
"""


# %% codecell
##############################################################
"""
vol_u1 = "https://www.cboe.com/us/options/"
vol_u2 = "market_statistics/historical_data/download/"
vol_u3 = "all_symbols/?reportType=volume&month=&year=2021&"
vol_u4 = "volumeType=adv&volumeAggType=monthly"
vol_u5 = "&exchanges=CBOE&exchanges=BATS&exchanges=C2&exchanges=EDGX"

cboe_vol_url = f"{vol_u1}{vol_u2}{vol_u3}{vol_u4}{vol_u5}"

cboe_vol_get = requests.get(cboe_vol_url)

all_syms_copy = pd.read_csv(
            BytesIO(cboe_vol_get.content),
            escapechar='\n',
            delimiter=',',
            delim_whitespace=False,
            skipinitialspace=False
            )

# Delete the bytes object
del cboe_vol_get
all_syms_vol = all_syms_copy.copy(deep=True)

# as = 'all syms'
all_syms_vol = dataTypes(all_syms_vol).df

syms_to_use = short_exp['Underlying'].value_counts().index.to_list()
all_syms_vol = all_syms_vol[all_syms_vol['Underlying'].isin(syms_to_use)].copy(deep=True)

# 'Trade Date' or 'Trade Month'
all_syms_sum = all_syms_vol.groupby(by=['Trade Date', 'Underlying']).sum()
all_syms_sum.dropna(inplace=True)
all_syms_sum.reset_index(inplace=True)

all_syms_sum['Average Daily Volume'].sort_values(ascending=True).shape

all_syms_vol.head(10)

all_syms_sum.head(100)

all_syms_vol['Trade Month'].value_counts()
"""
# %% codecell
##############################################################


# %% codecell
##############################################################


# %% codecell
##############################################################
mmo_only = mmo.mmo_df.copy(deep=True)
sym_only = mmo.sym_df.copy(deep=True)



report_date = datetime.date(2021, 2, 16)
td_vol_df = TradeVolume(report_date, 'con_volume')
td_vol_df = td_vol_df.vol_df

cols_to_int = ['firmQuantity', 'customerQuantity', 'marketQuantity']

td_vol_df[cols_to_int] = td_vol_df[cols_to_int].astype('int')

td_vol_df['totalVolume'] = td_vol_df['firmQuantity'] + td_vol_df['customerQuantity'] + td_vol_df['marketQuantity']

td_syms_df = td_vol_df[td_vol_df['underlying'].isin(mmo_syms)]

td_vol_df.dtypes

etfs_to_exclude = ['HYG', 'XLF', 'EEM', 'EWZ', 'TLT', 'VXX', 'RUT', 'ARKK', 'XLE', 'UVXY', 'EFA']
td_syms_df = td_syms_df[~td_syms_df['underlying'].isin(etfs_to_exclude)]

td_syms_df.sort_values(by='customerQuantity', ascending=False).head(200)

# Get memory usage
mmo_df2.info(memory_usage='deep')

# %% codecell
##############################################################




# %% codecell
##############################################################
profiler.start()
mmo_clean_df = cleanMmo(mmo)
profiler.stop()

print(profiler.output_text(unicode=True, color=True))
