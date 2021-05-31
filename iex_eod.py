"""
Analyze IEX End of Day Quotes.
"""
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

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

#from data_collect.iex_routines import iexClose
#importlib.reload(sys.modules['data_collect.iex_routines'])
#from data_collect.iex_routines import iexClose

from multiuse.help_class import baseDir, dataTypes, getDate, local_dates
# importlib.reload(sys.modules['multiuse.help_class'])
# from multiuse.help_class import baseDir, dataTypes, getDate, local_dates

from data_collect.iex_class import readData, urlData
from data_collect.iex_routines import iexClose, histPrices

from data_collect.hist_prices import HistPricesV2
importlib.reload(sys.modules['data_collect.hist_prices'])
from data_collect.hist_prices import HistPricesV2

# Display max 50 columns
pd.set_option('display.max_columns', 100)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################
dt = getDate.query('iex_eod')
pd.bdate_range(date(dt.year, 1, 2), dt)
dt
aapl = HistPricesV2('AAPL')

wt = serverAPI('redo', val='warrants')

cboe = serverAPI('redo', val='cboe_close')


# %% codecell
##################################

# Someone bought/sold 800 calls at $7 strike for RIG 2022

all_syms = serverAPI('all_symbols').df

cols_to_exclude = ['cef', 'et', 'oef', 'ps']

dt = getDate.query('iex_eod')
bd_range = pd.bdate_range(date(dt.year, 1, 2), dt)

times_need = bd_range[~bd_range.isin(df['date'])]
dts_need = [bd.date().strftime('%Y%m%d') for bd in times_need]



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
