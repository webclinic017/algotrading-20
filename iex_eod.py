"""
Analyze IEX End of Day Quotes.
"""
# %% codecell
##################################
import requests
import pandas as pd
from pandas.tseries.offsets import BusinessDay
import numpy as np
import sys
from datetime import date
import datetime
import os
import importlib
from dotenv import load_dotenv
from io import StringIO, BytesIO
from json import JSONDecodeError
from datetime import date
from nested_lookup import nested_lookup

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

#from data_collect.iex_routines import iexClose
#importlib.reload(sys.modules['data_collect.iex_routines'])
#from data_collect.iex_routines import iexClose

from multiuse.help_class import baseDir, dataTypes, getDate, local_dates
from data_collect.iex_class import readData, urlData

from data_collect.iex_routines import iexClose, histPrices
importlib.reload(sys.modules['data_collect.iex_routines'])
from data_collect.iex_routines import iexClose, histPrices

# Display max 50 columns
pd.set_option('display.max_columns', 100)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################

iex_eod = serverAPI('iex_quotes_raw')
iex_df = iex_eod.df.T.copy(deep=True)

iex_df.head(10)

url = "https://algotrading.ventures/api/v1/prices/eod/all"
get = requests.get(url).json()


url = "https://algotrading.ventures/api/v1/prices/combined/dt"
get = requests.get(url).json()
iex_df = pd.DataFrame(get)

iex_df[iex_df['symbol'] == 'ITOS']

iex_df.head(5)

# %% codecell
##################################
# %% codecell
##################################

url = "https://algotrading.ventures/api/v1/symbols/data/SHIPW"
get = requests.get(url)

get_json = get.json()
shipw_df = pd.read_json(get_json['iex_close'])
shipw_df.head(10)

latest = shipw_df.sort_values(by='latestUpdate', ascending=False).head(1)
pd.to_datetime(latest['latestUpdate'], unit='ms')

iex_last = pd.to_datetime(shipw_df['latestUpdate'], unit='ms')
iex_last.dt.date

shipw_df['latestUpdate']

shipw_df.dtypes


'mr_close'

# %% codecell
##################################

st = serverAPI('st_watch').df
batch = st.T.symbols.tolist()
payload = {'batch': batch}
url = 'https://algotrading.ventures/api/v1/symbols/get/batch'
get = requests.get(url, params=payload)

payload

# %% codecell
##################################

url = "https://algotrading.ventures/api/v1/redo/functions/sector_perf"
requests.get(url)





# %% codecell
##################################

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

url = "https://algotrading.ventures/api/v1/symbols/warrants/cheapest"
get = requests.get(url)
get_json = get.json()
wt_df = pd.DataFrame(get_json)


wt_df['key'].value_counts()

wt_ser_65 = (wt_df['key'].value_counts()[wt_df['key'].value_counts() > 65])
wt_ser_10 = (wt_df['key'].value_counts()[wt_df['key'].value_counts() < 10])


# %% codecell
##################################




# %% codecell
##################################


all_symbols = serverAPI('all_symbols').df
cs_syms = all_symbols[all_symbols['type'] == 'cs']['symbol'].tolist()
cs_syms

all_symbols['type'].value_counts()

iex_eod = serverAPI('iex_comb_today').df
iex_eod['vol/avg'] = (iex_eod['volume'] / iex_eod['avgTotalVolume'] * 100).round(2)
iex_eod.sort_values(by=['vol/avg'], ascending=False).head(50)

iex_eod.head(10)


top_vol_df  = serverAPI('cs_top_vol').df
top_vol_df.head(10)

wt_list = all_symbols[all_symbols['type'] == 'wt'][['symbol', 'name']]


wt_list = all_symbols[all_symbols['type'] == 'wt']['symbol'].tolist()
wt_list

wt_df = pd.merge(iex_df, wt_list, on=['symbol'])

wt_df.dropna(axis=0, subset=['iexClose'], inplace=True)

wt_df.sort_values(by=['iexClose'], ascending=True)[['symbol', 'companyName', 'iexClose']].head(100)


wt_df.head(10)

all_symbols['type'].value_counts()

all_symbols


# %% codecell
##################################
from datetime import timedelta

new_symbols = serverAPI('new_syms_all').df
new_symbols['dt'] = pd.to_datetime(new_symbols['date'], unit='ms')

mr = new_symbols[new_symbols['dt'] == new_symbols['dt'].max()]
mr_1 = new_symbols[new_symbols['dt'] == (new_symbols['dt'].max() - timedelta(days=1))]

df_diff = (mr.set_index('symbol')
            .drop(mr_1['symbol'], errors='ignore')
            .reset_index(drop=False))

df_diff

mr.shape
mr_1.shape
new_symbols.shape
mr.dtypes
new_symbols['dt'].value_counts()
new_symbols.head(10)

# %% codecell
##################################
load_dotenv()
sym = 'OCGN'

url = 'https://cloud-sse.iexapis.com/stable/news-stream'
payload = ({'token': os.environ.get("iex_publish_api"),
            'symbols': sym})
get = requests.get(url, params=payload)

get = urlData(f"/stock/{sym}/news/last/{1}")
get.df
"""
val = 'cboe_close'
url = f"https://algotrading.ventures/api/v1/prices/eod/{val}"
get = requests.get(url)

"""
# %% codecell
##################################
import glob
base_dir = baseDir().path
fpath_base = f"{base_dir}/intraday/2021/*/**"
choices = glob.glob(fpath_base)

for choice in choices:
    os.remove(choice)


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

# GET /stock/{symbol}/quote/{field}




# %% codecell
##################################
