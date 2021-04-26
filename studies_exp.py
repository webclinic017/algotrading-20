
# %% codecell
#############################################
import glob
from datetime import date
import importlib
import sys

import pandas as pd
from pandas.tseries.offsets import BusinessDay

from api import serverAPI

from studies.studies import regStudies
from studies.drawings import makeDrawings

from multiuse.help_class import baseDir, getDate, dataTypes
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import baseDir, getDate, dataTypes



# Display max 50 columns
pd.set_option('display.max_columns', 100)
# Display maximum rows
pd.set_option('display.max_rows', 500)
# %% codecell
#############################################

base_path = f"{baseDir().path}/StockEOD/{date.today().year}"
globs = glob.glob(f"{base_path}/*/**")
globs

for path in globs:
    df = pd.read_json(path, compression='gzip')
    break

df = regStudies(df).df
df = makeDrawings(df).df

df['localMin_5'].value_counts()

df['localMin_10'].value_counts()

df.head(10)

df.shape
# %% codecell
#############################################

df = serverAPI('iex_quotes_raw').df
df.shape

iex_df = dataTypes(df).df

# 27 mbs with data type adjustments
iex_df.info(memory_usage='deep')
# 154 mbs without data type adjustments
df.info(memory_usage='deep')
import numpy as np
np.finfo('float32')
np.finfo('float16')

np.finfo('float16').max
np.finfo('float32').max
# %% codecell
#############################################
iex_df.shape



# iex_df['closeDate'] = pd.to_datetime(iex_df['closeTime'], unit='ms').dt.date
iex_df['closeDate'] = pd.to_datetime(iex_df['lastTradeTime'], unit='ms').dt.date
iex_df['lastTradeTime'].isna().sum()
iex_df['closeTime'].isna().sum()
iex_df['latestTime'].isna().sum()

iex_df['iexOpen'].isna().sum()
iex_df['iexClose'].isna().sum()
iex_df['high'].isna().sum()
iex_df.dtypes

# Get all the symbols that are warrants
syms_type = serverAPI('all_symbols').df

syms_wt = syms_type[syms_type['type'] == 'wt']['symbol'].tolist()

cols_to_filter = (['symbol', 'latestPrice', 'changePercent', 'volume',
                   'previousVolume', 'avgTotalVolume', 'marketCap',
                   'week52High', 'week52Low', 'latestTime', 'ytdChange', 'iexOpen', 'iexClose', 'closeDate'])
iex_filter_df = iex_df[cols_to_filter].copy(deep=True)
# Get all the symbols that aren't warrants
iex_filter_df = iex_filter_df[~iex_filter_df['symbol'].isin(syms_wt)]


iex_filter_df['vol/prev'] = (iex_filter_df['volume'] / iex_filter_df['previousVolume']).round(1)
iex_filter_df['vol/avg'] = (iex_filter_df['volume'] / iex_filter_df['avgTotalVolume']).round(1)
iex_filter_df['prevVol/avg'] = (iex_filter_df['previousVolume'] / iex_filter_df['avgTotalVolume']).round(1)

date_list = []
for x in range(1, 5):
    date_list.append((date.today() - BusinessDay(n=x)).date())


(iex_filter_df[(iex_filter_df['latestPrice'] < 15)
               & (iex_filter_df['week52High'] < 25)
               & (iex_filter_df['iexClose'] > iex_filter_df['iexOpen'])
               & (iex_filter_df['closeDate'].isin(date_list))
               # (iex_filter_df['changePercent'] > .05)
               ].sort_values(by=['vol/avg', 'ytdChange'], ascending=False).head(25).reset_index(drop=True))

# %% codecell
#############################################

# Fibonacci retracements and extensions - if none of the prices are within
# the levels or within 5% of the closest level, indidcate that levels
# are no longer providing anything useful




# %% codecell
#############################################

last_date = getDate.query('iex_close')
cols_to_show = ['symbol', 'companyName', 'latestPrice', 'week52High', 'week52Low', 'ytdChange']
iex_last_df = iex_df[iex_df['closeDate'] == last_date].copy(deep=True)



iex_last_df.sort_values(by=['ytdChange'], ascending=False).head(25)[cols_to_show]








# %% codecell
#############################################
