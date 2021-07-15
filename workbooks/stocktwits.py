"""
Stocktwits Data.
- Rate limit of 200 get requests/hour with unauthenticated
- Rate limit of 400 get requests/hour with authenticated
- Unauthenticated is tied to IP address (rotating proxies)
"""
# %% codecell
#############################################################
import requests
import pandas as pd
import numpy as np
import json
from io import StringIO, BytesIO
import os.path
from pathlib import Path
import os
import glob
from copy import deepcopy
from dotenv import load_dotenv
import gzip

import sys
import importlib

import datetime
from datetime import date, timedelta, time

from nested_lookup import nested_lookup
from nested_lookup import get_occurrence_of_key as gok

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

from data_collect.iex_class import readData
importlib.reload(sys.modules['data_collect.iex_class'])

from multiuse.help_class import baseDir, getDate, dataTypes, local_dates
importlib.reload(sys.modules['multiuse.help_class'])

from data_collect.iex_routines import histPrices

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
#############################################################

# Do this twice a day, once in the morning, once at night
# Get the last 29 messages. That seems like it would be enough


# %% codecell
#############################################################
st_trend = serverAPI('st_trend')
st_df = st_trend.df.copy(deep=True)
sym_list = st_df['symbol'].value_counts().index.to_list()

# Get all data that isn't in the etf list
etf_list = readData.etf_list()
st_df = st_df[~st_df['symbol'].isin(etf_list['symbol'])]

sym_list = st_df['symbol'].value_counts().index.to_list()
# Remove all crypto or other unusual symbols in sym_list
[sym_list.remove(sym) for sym in sym_list if '.' in sym if ',' in sym]
# %% codecell
#############################################################

st_all = serverAPI('st_trend_all')

st_df = dataTypes(st_all.df).df.copy(deep=True)

st_df.info(memory_usage='deep')

st_df.head(1)

# %% codecell
#############################################################


# %% codecell
#############################################################

##################################################################
# st_trend = serverAPI('st_trend_today')
url = "https://algotrading.ventures/api/v1/stocktwits/trending/today/explore"
st_get = requests.get(url).json()
df = pd.DataFrame(st_get)



sym_list = st_df['symbol'].value_counts().index.to_list()

histPrices(sym_list)

# %% codecell
##################################################################
# pd.DataFrame(get.json(), index=[0])

get_watch = requests.get('https://algotrading.ventures/api/v1/stocktwits/watchlist')



watch = get_watch.json()


# %% codecell
###########################################################

# %% codecell
###########################################################

# %% codecell
###########################################################



# %% codecell
###########################################################

def st_watchlist_id():
    """Get watchlist id for authenticating user."""
    load_dotenv()
    watch_url = "https://api.stocktwits.com/api/2/watchlists.json"
    payload = {'access_token': os.environ.get("st_token")}
    stwits_get = requests.get(watch_url, params=payload).json()

    return stwits_get['watchlists'][0]['id']





# %% codecell
#############################################################
