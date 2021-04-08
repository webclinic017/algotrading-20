"""
Analyze IEX End of Day Quotes.
"""
# %% codecell
##################################
import requests
import pandas as pd
import numpy as np
import sys
from datetime import date
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
pd.set_option('display.max_columns', 10)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################

iex_eod = serverAPI('iex_quotes_raw')
iex_df = iex_eod.df.T.copy(deep=True)

# %% codecell
##################################

sym = 'OPTI'
hp = histPrices([sym])
hp.get_ytd_syms

hp.ld_dict['dl_ser']

hp.ld_dict['syms_dict_to_get'].keys()


# %% codecell
##################################

st = serverAPI('st_watch').df
batch = st.T.symbols.tolist()
payload = {'batch': batch}
url = 'https://algotrading.ventures/api/v1/symbols/data/get/batch'
get = requests.get(url, params=payload)
get.content

# %% codecell
##################################


all_df = pd.DataFrame()
for key in iex_json.keys():
    try:
        df = pd.DataFrame(iex_json[key])
        all_df = pd.concat([all_df, df])
    except ValueError:
        print(key)

# %% codecell
##################################

sym = 'SHIPZ'
get = requests.get('https://algotrading.ventures/api/v1/symbols/data/SHIPZ')
get_json = get.json()
iex_close = pd.read_json(get_json['iex_close'])
iex_hist = pd.read_json(get_json['iex_hist'])



iex_hist.dtypes


# %% codecell
##################################



for key in iex_json.keys():
    iex_json[key] = pd.DataFrame(iex_json[key])

items = list(iex_json.values())
this_df = pd.concat(items)

this_df.head(10)


this_df.dtypes


all_df = pd.DataFrame.from_dict(iex_json)
all_df.iloc[0]


# %% codecell
##################################

url = "https://algotrading.ventures/api/v1/symbols/warrants"
get = requests.get(url)
get_json = get.json()

wt_df = pd.DataFrame(get_json)

wt_df_mr = wt_df[wt_df['date'] == wt_df['date'].max()]

cols_to_keep = (['close', 'high', 'low', 'open', 'volume',
                 'label', 'date', 'changePercent', 'symbol'])

wt_df_mr = wt_df_mr[cols_to_keep]
wt_df_mr.reset_index(inplace=True, drop=True)
wt_df_mr.sort_values(by=['close'], ascending=True).head(25)


date_list = pd.to_datetime(wt_df_mr['date'].iloc[0], unit='ms').date()
# %% codecell
##################################


all_symbols = serverAPI('all_symbols').df

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
base_dir = baseDir().path
fpath = f"{base_dir}/tickers/all_symbols.gz"
all_symbols = pd.read_json(fpath, compression='gzip')
wt_list = all_symbols[all_symbols['type'] == 'wt']['symbol'].tolist()

histPrices(wt_list)



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


# %% codecell
##################################

# GET /stock/{symbol}/quote/{field}




# %% codecell
##################################
