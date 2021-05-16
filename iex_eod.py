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
from data_collect.iex_class import readData, urlData
importlib.reload(sys.modules['data_collect.iex_class'])
from data_collect.iex_class import readData, urlData

from data_collect.iex_routines import iexClose, histPrices
importlib.reload(sys.modules['data_collect.iex_routines'])
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

base_dir = baseDir().path
dt = getDate.query('iex_eod')
fpath_base = f"{base_dir}/StockEOD/{dt.year}/*/**.gz"
choices = glob.glob(fpath_base)

all_syms_fpath = f"{base_dir}/tickers/all_symbols.gz"
all_syms = pd.read_json(all_syms_fpath, compression='gzip')
sym_list = all_syms['symbol'].tolist()

for n in list(range(10)):
    histPrices([sym_list[n]])

aa_fpath = '/Users/unknown1/Algo/data/StockEOD/2021/a/_AA.gz'
aa_df = pd.read_json(aa_fpath, compression='gzip')

aa_df['date'].max()

aa_df = aa_df.sort_values(by='date', ascending=False).head(50)
aa_df.to_json(aa_fpath)

hp_aa = histPrices(['AA'])


hpv2_aa = HistPricesV2('AA')
hpv2_aa.df['date'].sort_values()
hpv2_aa.df['date'].shape

for sn, sym in enumerate(sym_list):
    if sn < 15:
        HistPricesV2(sym)
    else:
        break


redo_iex = serverAPI('redo', val='iex_close')

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

hist_prices = histPrices(['BNGO'], '2y')


all_StockEOD_fpaths = f"{base_dir}/StockEOD/*/**/***"
choices = glob.glob(all_StockEOD_fpaths)
choices

# %% codecell
##################################

# %% codecell
##################################

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



view = urlData('/time-series/advanced_right_to_purchase/VIEW?last=2')
view.df

url = f"{base_url}/stock/VIEWW/stats"
vieww_stats_get = requests.get(url, params=payload)
vieww_stats_json = vieww_stats_get.json()
vieww_stats_json

url = f"{base_url}/stock/VIEWW/company"
vieww_comp_get = requests.get(url, params=payload)
vieww_comp_json = vieww_comp_get.json()
vieww_comp_json



vieww_stats_get.content

load_dotenv()
base_url = os.environ.get("base_url")
payload = {'token': os.environ.get("iex_publish_api")}
url = f"{base_url}/time-series/advanced_right_to_purchase/APBCF?last=1"
get = requests.get(url, params=payload)
get.content

all_symbols = serverAPI('all_symbols').df
view_sym = all_symbols[all_symbols['symbol'].str.contains('VIEW')]
view_sym

BBG00XV49NV0

all_symbols.head(1)

all_symbols['type'].value_counts()
all_derivs = all_symbols[all_symbols['type'].isin(['wt', 'rt', 'ut'])].copy(deep=True)
all_derivs[all_derivs['type'] == 'ut']['name'].iloc[0]
all_derivs['name'].head(50)

cs_syms = all_symbols[all_symbols['type'] == 'cs']['symbol'].tolist()


all_symbols['type'].value_counts()

all_wts = all_symbols[all_symbols['type'] == 'wt'].copy(deep=True)
wts_exp = all_wts['name'].str[-10:-1]
wts_exp.head(5)



all_wts['name'].head(25)
all_wts.head(5)

# %% codecell
##################################

iex_eod = serverAPI('iex_comb_today').df



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

# %% codecell
##################################

"""
val = 'cboe_close'
url = f"https://algotrading.ventures/api/v1/prices/eod/{val}"
get = requests.get(url)

"""
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
