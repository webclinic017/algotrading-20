"""Analyze IEX End of Day Quotes."""
# %% codecell
##################################
import requests
import pandas as pd
import numpy as np
import sys
import glob
from datetime import date
import os
import importlib
from dotenv import load_dotenv
from json import JSONDecodeError
from datetime import date
import json

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

from studies.moving_averages import movingAverages
importlib.reload(sys.modules['studies.moving_averages'])
from studies.moving_averages import movingAverages

from multiuse.help_class import baseDir, dataTypes, getDate, local_dates, df_create_bins, RecordHolidays, check_size
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import baseDir, dataTypes, getDate, local_dates, RecordHolidays

from data_collect.iex_class import readData, urlData
from data_collect.iex_routines import iexClose, histPrices

from data_collect.hist_prices import HistPricesV2
importlib.reload(sys.modules['data_collect.hist_prices'])
from data_collect.hist_prices import HistPricesV2

from master_funcs.master_hist_prices import SplitGetHistPrices

from charting.plt_standard import plot_cols
importlib.reload(sys.modules['charting.plt_standard'])
from charting.plt_standard import plot_cols

# Display max 50 columns
pd.set_option('display.max_columns', 100)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################

url = f"https://algotrading.ventures/api/v1/symbols/data/AAPL"
get = requests.get(url).json()
df = pd.read_json(get['iex_hist'])

val = getDate.query('iex_eod')
val
url = f"https://algotrading.ventures/api/v1/prices/combined/{val}"
get = requests.get(url).json()
df = pd.DataFrame(get)
df = dataTypes(df).df.copy(deep=True)
cols = ['symbol', 'close', 'change', 'changePercent', 'volume', 'avgTotalVolume', 'today/avg_vol']


# df['avgTotalVolume'] = df['avgTotalVolume'].where(df['avgTotalVolume'] != 0, 1)
df_sub = df[(df['avgTotalVolume'] > 100) & (df['changePercent'] > .05)].copy(deep=True)
df_sub['today/avg_vol'] = ((df_sub['volume'] / df_sub['avgTotalVolume']).round(1) * 100)
df_sub.sort_values(by=['today/avg_vol'], ascending=False).head(50)[cols]
df['volume'].head(1)

df.head(1)
aapl = serverAPI('stock_data', symbol='AAPL').df
aapl_ma = movingAverages(aapl).df

# %% codecell
##################################
import requests
import gzip
import pandas as pd
import json
from io import StringIO

url = 'https://algotrading.ventures/api/v1/prices/eod/test'
get = requests.get(url)
# gzip_decom = gzip.decompress(get.content)
df = pd.read_json(gzip.decompress(get.content))
df = dataTypes(df).df

cols_to_include = ['symbol', 'latestTime', 'close', 'change', 'changePercent', 'volume', 'avgTotalVolume', 'today/avg_vol']
df_sub = df[(df['avgTotalVolume'] > 100) & (df['changePercent'] > .05)].copy(deep=True)
df_sub['today/avg_vol'] = ((df_sub['volume'] / df_sub['avgTotalVolume']).round(1) * 100)
df_sub.sort_values(by=['today/avg_vol'], ascending=False).head(50)[cols_to_include]

cols = df.columns
for col in cols:
    print(f"Column {col}: {df[col].isna().sum()}")


df.info(memory_usage='deep')


base_dir = baseDir().path
fpath = f"{base_dir}/iex_eod_quotes/combined/_2021_all_2021-07-16.gz"
df.to_json(fpath, compression='gzip')

"""
gzip_com = gzip_decom.decode('utf-8')
df = pd.read_csv(StringIO(gzip_com))
"""

# pd.read_csv is significantly worse than just pd.read_json from decompressed data
# Also takes up wayyyy more space. Good to know.

# %% codecell
##################################


df_ohlc = plot_cols(aapl_ma, vol=True, candle=True, moving_averages='cma')

# %% codecell
##################################


url_all = "https://algotrading.ventures/api/v1/prices/eod/all"
get = requests.get(url_all)
get.status_code



iex_eod_all = serverAPI('iex_quotes_raw').df


all_syms = serverAPI('all_symbols').df


# %% codecell
##################################

def get_this_year_bus_days():
    """Get a list of all of this years business days."""
    bus_days = getDate.get_bus_days()
    dt_today = date.today()
    bus_days = (bus_days[(bus_days['date'].dt.year == dt_today.year) &
                         (bus_days['date'].dt.dayofyear <
                          dt_today.timetuple().tm_yday)])
    return bus_days



# Start of a fibonacci retracement opportunity.
# Look for volume that is substantially higher than the last few days/weeks.
# Say 2x the average volume for the previous 2 months
# Significant up movement ~ greater than 3% let's call it.
# Look for corporate events like news article or something similar within 1 day.

# Look for overlap in social sentiment. One way to start doing this is to
# Get all the stocktwits data, then the all symbols data, and map up the CIKs for the underlying SPACs.


all_syms = serverAPI('all_symbols').df.copy(deep=True)
all_syms = dataTypes(all_syms, resolve_floats=True).df.copy(deep=True)

# %% codecell
#######################

all_iex_url = 'https://algotrading.ventures/api/v1/prices/combined/2021_all_2021-07-16'
all_iex_get = requests.get(all_iex_url)


all_iex_get.content
all_iex_json = all_iex_get.json()

all_iex = serverAPI('iex_quotes_raw').df


# Celery tasks is overwriting tasks_captain and I don't know why.
# Tried changing the names, verifying the code to no avail. No symlinks that I can find.
# This is only a problem with tasks_captain, and not with the other celery instances.


# %% codecell
#######################

deriv_list = ['wt', 'ut']

all_derivs = all_syms[all_syms['type'].isin(deriv_list)]

all_cs_wts = (all_syms[(all_syms['cik'].isin(all_derivs['cik'].tolist())) &
                       (all_syms['type'] == 'cs')])

all_cs_syms = all_cs_wts['symbol'].tolist()



all_st = serverAPI('st_trend_all')
all_st_df = all_st.df.copy(deep=True)
all_st_df = dataTypes(all_st.df).df


all_st_df.dropna(subset=['watchlist_count'], inplace=True)
all_st_df.reset_index(drop=True, inplace=True)

all_comb = all_st_df[all_st_df['symbol'].isin(all_cs_syms)].reset_index(drop=True)

df_counts = pd.DataFrame(all_comb['symbol'].value_counts() > 1).reset_index()
df_counts = df_counts[df_counts['symbol'] == True].copy(deep=True)
all_comb = all_comb[all_comb['symbol'].isin(df_counts['index'].tolist())]

all_ocgn = all_st_df[all_st_df['symbol'] == 'OCGN'].copy(deep=True)
all_ocgn['timestamp'] = pd.to_datetime(all_ocgn['timestamp'], unit='ms')
all_ocgn['date'] = all_ocgn['timestamp'].dt.date

all_ocgn_by_day = all_ocgn.groupby(by='date').count().reset_index()[['date', 'id']].rename(columns={'id': 'count'})

url = "https://algotrading.ventures/api/v1/symbols/data/OCGN"
sym = requests.get(url).json()
ocgn_hist = pd.read_json(sym['iex_hist'])
all_ocgn_by_day['date'] = pd.to_datetime(all_ocgn_by_day['date'])
ocgn_sub = pd.merge(ocgn_hist, all_ocgn_by_day, on='date').copy(deep=True)
ocgn_sub.set_index(keys='date', inplace=True)
import matplotlib.pyplot as plt
%matplotlib inline

# %% codecell
###########################################################################
ocgn_sub['fClose'].plot(label='Close', figsize=(16, 8))
ocgn_sub['count'].plot(label='Count', secondary_y=True)
plt.legend()

# %% codecell
###########################################################################

all_ocgn.plot(x='date', '')

all_ocgn['date'].min()



all_st_df_cols = {'id': np.uint16, 'symbol': 'category', 'watchlist_count': np.uint32, 'timestamp': }

all_st_df['watchlist_count'] = all_st_df['watchlist_count'].astype(np.uint32)
all_st_df.head(10)

"""
70,375,430
70,376,800
70,378,170
70,379,540
70,390,510
72,581,410
78,823,600
"""



serverAPI('redo', val='iex_close')

serverAPI('redo', val='check_size')

serverAPI('redo', val='scans_vol_avg')

serverAPI('redo', val='get_bus_days')

serverAPI('redo', val='split_get_hist_prices')

wt = serverAPI('redo', val='warrants')

cboe = serverAPI('redo', val='cboe_close')

hist_wts = serverAPI('redo', val='hist_warrants')

hist_prices_v2_sub = serverAPI('redo', val='iex_hist_v2_sub')

# This should be run at the beginning of every weekday

# %% codecell
##################################

# Someone bought/sold 800 calls at $7 strike for RIG 2022

# We probably want the last 50 holidays, the next 50 holidays, to run every 6 months

# %% codecell
##################################
redo_otc_syms = serverAPI('redo', val='otc_ref_data')
otc_syms = serverAPI('otc_syms').df

all_syms = serverAPI('all_symbols').df
all_syms = df_create_bins(all_syms)

all_syms.dtypes

base_dir = baseDir().path


new_syms = urlData('/ref-data/symbols')
new_syms_df = new_syms.df.copy(deep=True)
new_syms_df['type'].value_counts()

all_syms['type'].value_counts()

otc_syms = urlData('/ref-data/otc/symbols').df
otc_df = otc_syms.copy(deep=True)

all_syms['bins'].value_counts()

#  pd.qcut(df['ext price'], q=4)

otc_df['type'].value_counts()


cols_to_exclude = ['cef', 'et', 'oef', 'ps']

dt = getDate.query('iex_eod')
bd_range = pd.bdate_range(date(dt.year, 1, 2), dt)

times_need = bd_range[~bd_range.isin(df['date'])]
dts_need = [bd.date().strftime('%Y%m%d') for bd in times_need]



# %% codecell
##################################

all_syms['type'].value_counts()



# %% codecell
##################################


iex_eod = serverAPI('iex_quotes_raw')
iex_eod_df = dataTypes(iex_eod.df.copy(deep=True)).df
iex_df = iex_eod_df.copy(deep=True)

all_syms = serverAPI('all_symbols').df
all_wts = all_syms[all_syms['type'] == 'wt'].copy(deep=True)
cik_list = all_wts['cik'].tolist()

all_cs_wt = all_syms[(all_syms['cik'].isin(cik_list)) & (all_syms['type'] != 'wt')].copy(deep=True)
all_cs_wt.shape[0]
all_cs_wt['type'].shape[0]
all_cs_wt_na = all_cs_wt[all_cs_wt['type'].isna()]

all_cs_wt_na['name'].iloc[0]

all_cs_wt_na.iloc[0]['symbol']
all_cs_wt['type'].value_counts()
all_cs_wt[all_cs_wt['type'] == 'ps']['name']
all_cs_wt[all_cs_wt['type'] != 'cs']['name']
# MSCI and global funds have the same CIK number
# Units have type NaN, although not sure if these are only units



# %% codecell
##################################

# hist_prices = histPrices(['BNGO'], '2y')

base_dir = baseDir().path
fpath = f"{base_dir}/derivatives/cboe_symref/*"
choice = glob.glob(fpath)[0]

df = pd.read_json(choice, compression='gzip')
df = dataTypes(df).df
df.info(memory_usage='deep')
data = {'row_1': 1, 'row_2': 2}
ser = pd.Series(data)

# %% codecell
##################################
"""
Average volume = 30 day avg

ad - ADR
cs - Common Stock
cef - Closed End Fund
et - ETF
oef - Open Ended Fund
ps - Preferred Stock
rt - Right
struct - Structured Product
ut - Unit
wi - When Issued
wt - Warrant
empty - Other
"""
# %% codecell
##################################
"""
# Cheapest warrants
url = "https://algotrading.ventures/api/v1/symbols/warrants/cheapest"
get = requests.get(url)
get_json = get.json()
wt_df = pd.DataFrame(get_json)


wt_df['key'].value_counts()

wt_ser_65 = (wt_df['key'].value_counts()[wt_df['key'].value_counts() > 65])
wt_ser_10 = (wt_df['key'].value_counts()[wt_df['key'].value_counts() < 10])

Done:
-Being able to sort from highest to lowest price would be nice

To do:
-Warrant conversion ratio (2 warrant for 1 share/3 warrants for 1 share, etc) is a must
-Exercise/Strike price, since not all of them are SPAC warrants with 11.50 strikes
-Expiration date would be huge
-Dual chart/side by side for the underlying stock AND the warrants simultaneously so its easier to gauge how much impact volatility and other factors have on the warrant price movement.
-Maybe build in an options contract calculator to figure out the theta value of the warrants so you can quickly gauge if they're underpriced or not

"""

# %% codecell
##################################


# %% codecell
##################################

view = urlData('/ref-data/figi?figi=BBG00XV49NV0')
view_df = view.df.copy(deep=True)
view_df

url = f"{base_url}/stock/vieww/company"
get = requests.get(url, params=payload)
get.json()


# %% codecell
##################################

# %% codecell
##################################

# %% codecell
##################################

# %% codecell
##################################



# %% codecell
##################################
true = True
false = False
ind = 'rsi'
sym = 'OCGN'
range = '1M'
per = 14

def get_technical_hist(ind, sym, range):
    """Get historical technical indicator data."""
    # ind = indicator, defined by iex. sym = 'OCGN' or similar
    # per = range defined by IEX chart endpoint
    payload = ({'token': os.environ.get("iex_publish_api"),
                'lastIndicator': false,
                'indicatorOnly': true,  # Only show the indicator
                'chartByDay': true, 'period': per})
    base_url = os.environ.get("base_url")
    url = f"{base_url}/stock/{sym}/indicator/{ind}?range={range}"
    get = requests.get(url, params=payload)
    get_json = get.json()
    get.content

    df_chart = pd.DataFrame(get_json['chart'])
    df_chart['rsi'] = pd.DataFrame(get_json['indicator']).T

    return df_chart

df_last = get_technical_hist(ind, sym, per)

fpath = f"{base_dir}/intraday/2021/{sym.lower()[0]}/_{sym}.gz"
df_chart.to_json(fpath, compression='gzip')

df_chart.tail(10)
# 365 minutes in the trading day
df_chart.shape
df_ind.shape[0]/ 60
df_ind

# %% codecell
##################################


# %% codecell
##################################


# %% codecell
##################################
