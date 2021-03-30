"""
Can we do free batch processing from the OCC?

Flex Number Prefix:
1 = American-style equity. For index products, this also means it is a.m. settled.
2 = European-style equity. For index products, this also means it is a.m. settled.
3 = American-style index, p.m. settled.
4 = European-style index, p.m. settled.

AMEX = NYSE
"""

# %% codecell
############################################################
import os
import json
from json import JSONDecodeError
from io import StringIO, BytesIO
import glob
import importlib
import sys

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

"""
from options import DerivativeExpirations, DerivativesHelper
importlib.reload(sys.modules['options'])
from options import DerivativeExpirations, DerivativesHelper
"""


from data_collect.iex_class import expDates, marketHolidays
importlib.reload(sys.modules['data_collect.iex_class'])
from data_collect.iex_class import expDates, marketHolidays

from data_collect.theocc_class import tradeVolume, occFlex
importlib.reload(sys.modules['data_collect.theocc_class'])
from data_collect.theocc_class import tradeVolume, occFlex

from multiuse.help_class import dataTypes, getDate

import xml.etree.ElementTree as ET

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI


# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 200)

# %% codecell
############################################################
report_date = getDate.which_fname_date()
occ_flex_oi = occFlex('OI', 'E', report_date)

flex_oi_df = occ_flex_oi.df.copy(deep=True)

flex_oi_df.dtypes


flex_oi_df.shape
flex_oi_df.head(10)


occ_flex_pr = occFlex('PR', 'E', report_date)
flex_pr_df = occ_flex_pr.df.copy(deep=True)

flex_pr_df.head(10)

flex_pr_df.dtypes

# %% codecell
##############################################################


report_date = getDate.which_fname_date()

report_date = datetime.date(2021, 3, 26)
td_vol = tradeVolume(report_date, 'con_volume', fresh=True).vol_df

td_vol_last = td_vol.copy(deep=True)

td_vol_last.head(10)


cboe_last = serverAPI('cboe_mmo_exp_last').df
for key in cboe_last.keys():
    cboe_df = cboe_last[key]
    break

td_vol_last.rename(columns={'contdate': 'expDate', 'underlying': 'Underlying'}, inplace=True)
td_vol_last.drop(columns=['pkind', 'exId'], inplace=True)

my_watch = serverAPI('st_watch').df.T
my_watch_syms = my_watch['symbols'].unique()

both_df = pd.merge(td_vol_last, cboe_df, how='inner', on=['Underlying', 'expDate'])
both_df.head(10)

my_syms_df = both_df[both_df['Underlying'].isin(my_watch_syms.tolist())].copy(deep=True)

my_syms_td_df = td_vol_last[td_vol_last['Underlying'].isin(my_watch_syms.tolist())].copy(deep=True)
# my_syms_td_df.head(10)
cols_to_int = ['cQuant', 'fQuant', 'mQuant']
my_syms_td_df[cols_to_int] = my_syms_td_df[cols_to_int].astype(np.uint16)

td_my_agg = my_syms_td_df.groupby(by=['Underlying', 'expDate']).sum().reset_index()
td_my_agg = td_my_agg.dropna()

my_syms_td_df.dtypes

td_my_agg

trade_vol = tradeVolume(report_date, 'trade_volume', fresh=True).vol_df



trade_vol['underlying'].value_counts().index



td_my_agg.shape

td_vol_last.shape


both_df['Underlying'].value_counts()

=both_df[both_df['Underlying'] == '']

my_syms_df.head(10)

td_vol_df.info(memory_usage='deep')

# %% codecell
##############################################################
trend_url = "https://algotrading.ventures/api/v1/stocktwits/trending"
t_get = requests.get(trend_url)
trend_test = json.load(BytesIO(t_get.content))

fpath_to_use = '/var/www/algo_flask/data/stocktwits/trending/2021-02-24.gz'

trend_df = pd.DataFrame(trend_test[fpath_to_use])
trend_df['dt'] = pd.to_datetime(trend_df['timestamp'], unit='ms')
trend_df['hour'] = trend_df['dt'].dt.hour
trend_df['minute'] = trend_df['dt'].dt.minute

trend_df.drop(columns=['timestamp', 'dt'], inplace=True)

trend_pre = trend_df[(trend_df['hour'] <= 9) & (trend_df['minute'] < 30)].copy(deep=True)
trend_am = trend_df[(trend_df['hour'] < 12) & (trend_df['hour'] >= 9) & (trend_df['minute'] >= 30)].copy(deep=True)
trend_pm = trend_df[(trend_df['hour'] > 12) & (trend_df['hour'] < 16)].copy(deep=True)
trend_post = trend_df[trend_df['hour'] >= 16].copy(deep=True)

trend_df.head(10)

# %% codecell
##############################################################

social_trade = pd.merge(trend_df['symbol'], td_vol_df, on='symbol', how='inner').copy(deep=True)
social_trade['totVol'] = social_trade['cQuant'] + social_trade['fQuant'] + social_trade['mQuant']
social_trade.reset_index(inplace=True, drop=True)
social_trade.drop_duplicates(subset=['symbol', 'contdate'], inplace=True)

social_trade.sort_values(by=['totVol'], ascending=False).head(10)

social_group = social_trade.groupby(by=['symbol', 'contdate']).sum()
social_group.reset_index(inplace=True)
social_group.dropna(axis=0, inplace=True)

social_group.sort_values(by=['totVol'], ascending=False).head(100)


exp_dates = pd.to_datetime(list(set(social_group['contdate'].value_counts().index.to_list())), yearfirst=True).date
exp_dict = {}

exp_dict['short'] = exp_dates[exp_dates < (date.today() + timedelta(days=45))].astype(str)
exp_dict['med'] = (exp_dates[(exp_dates > (date.today() + timedelta(days=45))) &
                           (exp_dates < (date.today() + timedelta(days=180)))]).astype(str)
exp_dict['long'] = exp_dates[exp_dates > (date.today() + timedelta(days=180))].astype(str)

social_short = social_group[social_group['contdate'].isin(exp_dict['short'])]
social_med = social_group[social_group['contdate'].isin(exp_dict['med'])]
social_long = social_group[social_group['contdate'].isin(exp_dict['long'])]

social_short


social_group.head(10)


# %% codecell
##############################################################

trade_vol = tradeVolume(report_date, 'trade_volume', fresh=True).vol_df
trade_vol_df = trade_vol.copy(deep=True)



# %% codecell
##############################################################



# %% codecell
##############################################################






# %% codecell
##############################################################
