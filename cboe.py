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

from data_collect.iex_routines import dailySymbols
importlib.reload(sys.modules['data_collect.iex_routines'])
from data_collect.iex_routines import dailySymbols

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
pd.set_option('display.max_rows', 200)

# %% codecell
##############################################################
"""
AD = ADR = American depository receipt - represents shares in a foreign entity
ET = ETF
PS = Preferred stock
WT = Warrant
Struct = Structured Product

CBOE Market Making:
Data for 2021-02-19 to 2021-02-25 inclusive. - Feb 26th and Feb 27th data access.
"""
getDate.query('cboe')

cboe = serverAPI('cboe_mmo_top')
cboe_df = cboe.df.copy(deep=True)
cboe_df.dropna(axis=0, inplace=True)

from pandas.tseries.offsets import BusinessDay
bs = BusinessDay(n=1)

cboe_df['date_dt'] = pd.to_datetime(cboe_df['dataDate'])
cboe_df['date_df'] = (cboe_df['date_dt'] + bs).dt.date

my_watchlist = serverAPI('st_watch')
stw_df = my_watchlist.df.copy(deep=True).T

my_watch = stw_df['symbols'].to_list()

cboe_my = cboe_df[cboe_df['Underlying'].isin(my_watch)].copy(deep=True)
cboe_my.reset_index(inplace=True, drop=True)
cboe_my['expDateDT'] = pd.to_datetime(cboe_my['expDate'])

cboe_my_today = cboe_my[cboe_my['date_df'] == date.today() - timedelta(days=1)].copy(deep=True)
cboe_my_today.reset_index(inplace=True, drop=True)

new_df = pd.DataFrame()
# r = pd.DatetimeIndex(cboe_my['expDate'].value_counts(ascending=True).index, yearfirst=True)
# r = pd.date_range(start=cboe_my_today.expDate.min(), end=cboe_my_today.expDate.max(), freq='7D')
r = pd.to_datetime(cboe_my['expDate']).value_counts().index.values
for sym in cboe_my_today['Underlying'].value_counts().index:

    mod_c = cboe_my_today[(cboe_my_today['Underlying'] == sym) & (cboe_my_today['side'] == 'C')].copy(deep=True)
    mod_p = cboe_my_today[(cboe_my_today['Underlying'] == sym) & (cboe_my_today['side'] == 'P')].copy(deep=True)
    # mod.reset_index(inplace=True, drop=True)
    # print(mod)

    try:
        mod_c = (mod_c.set_index('expDateDT')
                  .reindex(r, fill_value=1)
                  .reset_index())
        mod_c['Underlying'] = sym

        mod_p = (mod_p.set_index('expDateDT')
                  .reindex(r, fill_value=1)
                  .reset_index())
        mod_p['Underlying'] = sym
        # print(mod)
    except ValueError as ve:
        print(sym)

    new_df = pd.concat([new_df, mod_c, mod_p]).copy(deep=True)


# %% codecell
##############################################################

cboe = requests.get("https://algotrading.ventures/api/v1/cboe/mmo/st/me/vue")

cboe_df = pd.DataFrame(cboe.json()).T

cboe_df['dataDate'].value_counts()

cboe_df.head(10)

my_watch = cboe_df['symbols'].to_list()

cboe_df.shape

# %% codecell
##############################################################

# break
new_df.shape
new_df.groupby(by=['Underlying']).count()


new_df

mod.set_index('expDateDT').reindex(r)

r

mod.head(10)

# mmo = cboeData('mmo')
# clean_mmo = cleanMmo(mmo)

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
# Write function to get and display top 100 short term symbols

from data_collect.econ_class import yahooTbills

tb_df = yahooTbills().df.copy(deep=True)
tb_df.head(10)

base_url = "https://algotrading.ventures/api/v1"
url = f"{base_url}/econ/treasuries"

get = requests.get(url)

get.content
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

"""
sym_df['yr'] = sym_df['yr'].astype('int8')
cols_to_category = ['Symbol', 'Underlying', 'exchange']
sym_df[cols_to_category] = sym_df[cols_to_category].astype('category')


# Keep only the next few years
sym_df['yr'] = sym_df['yr'].astype('int8')
years_to_get = [20, 21, 22, 23, 24, 25]
sym_df = sym_df[sym_df['yr'].isin(years_to_get)]


sym_df.dtypes
"""
# %% codecell
##############################################################




df.info(memory_usage='deep')





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

mmo_syms = mmo_df2['Underlying'].value_counts()[0:250].index.to_list()
len(mmo_syms)
len(mmo_syms)

mmo_syms
# .index.to_list()
# comb_df = pd.merge(mmo_df2, df_vol_df, on=['], how='left')
# mmo_df2['missedOpportunity'] = mmo_df2['Missed Liquidity'] / mmo_df2['Cboe ADV']

mmo_df2.shape


mmo_df2.sort_values(by=['Liquidity Opportunity'], ascending=False).head(200)
mmo_df2[mmo_df2['Underlying'] == 'TXMD'].head(50)
mmo_df2.sort_values(by=['Missed Liquidity'], ascending=False).head(50)
mmo_df2[mmo_df2['Underlying'] == 'XIN'].head(10)

mmo_df2.sort_values(by=['missedOpportunity'], ascending=False).head(50)
mmo_df2.sort_values(by=['Liquidity Opportunity'], ascending=False).head(50)

mmo_df2.rename(columns={'Underlying': 'underlying'}, inplace=True)


# %% codecell
##############################################################
profiler.start()
mmo_clean_df = cleanMmo(mmo)
profiler.stop()

print(profiler.output_text(unicode=True, color=True))
