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

from data_collect.iex_routines import iexClose
importlib.reload(sys.modules['data_collect.iex_routines'])
from data_collect.iex_routines import iexClose

# Display max 50 columns
pd.set_option('display.max_columns', 10)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################

iex_eod = serverAPI('iex_quotes_raw')
iex_df = iex_eod.df.T.copy(deep=True)

iex_json = iex_eod.df.json()



iex_eod.df.head(10)


iex_df.head(10)


iex_eod.df.shape

fpath = '/Users/unknown1/Algo/data/iex_eod_quotes/combined/2021-03-29.gz'
iex_df = pd.read_json(fpath, compression='gzip')


iex_df.info(memory_usage='deep')
iex_df.head(10)
# iex_eod.df.to_json(fpath, compression='gzip')

iex_eod.df.head(10)


# %% codecell
##################################

iex_get = requests.get('https://algotrading.ventures/api/v1/prices/eod/all')
iex_json = iex_get.json()


all_df = pd.DataFrame()
for key in iex_json.keys():
    try:
        df = pd.DataFrame(iex_json[key])
        all_df = pd.concat([all_df, df])
    except ValueError:
        print(key)

# %% codecell
##################################


# %% codecell
##################################



for key in iex_json.keys():
    iex_json[key] = pd.DataFrame(iex_json[key])

items = list(iex_json.values())
this_df = pd.concat(items)


this_df.dtypes


all_df = pd.DataFrame.from_dict(iex_json)
all_df.iloc[0]


# %% codecell
##################################
import glob

base_dir = baseDir().path
fpath = f"{base_dir}/iex_eod_quotes/{date.today().year}/*/**.gz"
choices = glob.glob(fpath)

my_list = []
for choice in choices:
    my_list.append(pd.read_json(choice, compression='gzip'))

# Concatenate all dataframes
all_df = pd.concat(my_list)
all_df.reset_index(inplace=True, drop=True)
# Convert datatypes to minimize space
all_df = dataTypes(all_df).df

fpath = f"{base_dir}/iex_eod_quotes/combined/{getDate.query('cboe')}"



all_df.head(10)

# %% codecell
##################################


all_symbols = serverAPI('all_symbols').df
wt_list = all_symbols[all_symbols['type'] == 'wt'][['symbol', 'name']]

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


# %% codecell
##################################

# GET /stock/{symbol}/quote/{field}




# %% codecell
##################################
