"""
Analyzing options trades from the occ
"""

# %% codecell
############################################################
import os
import os.path

import importlib
import sys
import xml.etree.ElementTree as ET

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

import datetime
from datetime import date, timedelta, time

from charset_normalizer import CharsetNormalizerMatches as CnM

from theocc_class import OccFlex, TradeVolume
importlib.reload(sys.modules['theocc_class'])
from theocc_class import OccFlex, TradeVolume

from iex_class import readData
importlib.reload(sys.modules['iex_class'])
from iex_class import readData
# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', None)

# %% codecell
############################################################
report_date = datetime.date(2021, 2, 11)
report_date = date.today()
report_date

occ_flex_oi = OccFlex('OI', 'E', report_date)
flex_oi_df = occ_flex_oi.occ_df.copy(deep=True)

all_symbols_df = readData.all_iex_symbols()
etf_df = readData.etf_list()

flex_oi_df_noetf = flex_oi_df[~flex_oi_df['SYMBOL'].isin(etf_df['symbol'].values)]

flex_oi_df_noetf.info(memory_usage='deep')

flex_oi_df_noetf.head(10)

# %% codecell
############################################################
report_date = datetime.date(2021, 2, 17)
con_vol = TradeVolume(report_date, 'con_volume')
con_df = con_vol.vol_df.copy(deep=True)

cols_to_cat = ['symbol', 'pkind', 'exchangeName', 'contdate', 'exchangeId', 'actdate', 'underlying']
con_df[cols_to_cat] = con_df[cols_to_cat].astype('category')
cols_to_int8 = ['customerQuantity', 'marketQuantity', 'firmQuantity']
con_df[cols_to_int8] = con_df[cols_to_int8].astype(np.int8)

con_df.info(memory_usage='deep')
con_df.head(10)

# %% codecell
############################################################

report_date = datetime.date(2021, 2, 17)
con_vol = TradeVolume(report_date, 'con_volume')
con_df = con_vol.vol_df.copy(deep=True)

sys.getsizeof(con_df) / 1000000
sys.getsizeof(occ_flex_oi.occ_df) / 1000000

con_df['expirationDate'] = pd.to_datetime(con_df['contdate'], infer_datetime_format=True)
con_df['expirationDate'] = con_df['expirationDate'].dt.strftime("%Y%m%d")

con_df.dtypes
con_df.shape

con_df.head(10)


oi_df['OI'] = pd.to_numeric(oi_df['OI'])
occ_cols = [col.lower() for col in oi_df.columns]
oi_df.columns = occ_cols

oi_df.head(10)

oi_am_df = oi_df[oi_df['style'] == 1]


oi_sum = oi_am_df.groupby(by=['symbol', 'expirationdate']).sum()
oi_sum.reset_index(inplace=True, drop=False)
oi_sum.rename(columns={'expirationdate': 'expirationDate', 'symbol': 'underlying'}, inplace=True)

oi_sum.head(10)

comb_df = pd.merge(con_df, oi_sum, on=['underlying', 'expirationDate'], how='left').copy(deep=True)
comb_df.rename(columns={'firmQuantity': 'firmQ', 'customerQuantity': 'customQ', 'marketQuantity': 'marketQ'}, inplace=True)

comb_df['totalVolume'] = comb_df['firmQ'] + comb_df['customQ'] + comb_df['marketQ']
comb_df.dropna(axis=0, inplace=True)

oi_df.shape
oi_sum.shape
comb_df.shape

# Combined unique symbols
len(set(comb_df['underlying'].value_counts().keys()))
len(set(oi_df['symbol'].value_counts().keys()))
len(set(con_df['underlying'].value_counts().keys()))

comb_df = comb_df[~comb_df['contdate'].isin(['2021-02-12'])]
comb_df['v/oi'] = comb_df['totalVolume'] / comb_df['oi']
comb_df['c/tv'] = comb_df['customQ'] / comb_df['totalVolume']


high_voi = (comb_df[(comb_df['v/oi'] > 3) &
                   (comb_df['totalVolume'] > 1000) &
                   (comb_df['c/tv'] < .25)])


high_voi.sort_values(by=['v/oi'], ascending=False).head(50)

comb_df.sort_values(by=['symbol', 'expirationDate']).head(10)

# %% codecell
##############################################################
report_date = datetime.date(2021, 2, 12)
trade_vol = TradeVolume(report_date, 'trade_volume')
trade_vol_df = trade_vol.vol_df

trade_vol_df.head(10)

con_df.head(10)

# %% codecell
##############################################################
