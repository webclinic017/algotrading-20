"""
Let's have some fun with stocktwits data
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
from multiuse.help_class import baseDir, getDate
importlib.reload(sys.modules['multiuse.help_class'])

# %% codecell
#############################################################

# Do this twice a day, once in the morning, once at night
# Get the last 29 messages. That seems like it would be enough


# %% codecell
#############################################################
st_trend = serverAPI('st_trend')
st_df = st_trend.df.copy(deep=True)

# Get all data that isn't in the etf list
etf_list = readData.etf_list()
st_df = st_df[~st_df['symbol'].isin(etf_list['symbol'])]

sym_list = st_df['symbol'].value_counts().index.to_list()
# Remove all crypto or other unusual symbols in sym_list
[sym_list.remove(sym) for sym in sym_list if '.' in sym if ',' in sym]
# %% codecell
#############################################################
base_dir = f"{baseDir().path}/StockEOD"
year = date.today().year
syms_path_dict = {}
for sym in sym_list:
    syms_path_dict[sym] = f"{base_dir}/{year}/{sym.lower()[0]}/_{sym}.gz"

load_dotenv()
true = True
base_url = os.environ.get("base_url")
payload = ({'token': os.environ.get("iex_publish_api"),
            'chartCloseOnly': true,
            'chartByDay': true})

# Empty list for syms that we got data for
syms_data = []
# Empty list for syms that we could not get data
syms_no_data = []
for sym, path in syms_path_dict.items():
    url = f"{base_url}/stock/{sym}/chart/ytd"
    get = requests.get(url, params=payload)
    try:
        df = pd.read_json(BytesIO(get.content))
        df.to_json(path, compression='gzip')
        syms_data.append(sym)
    except ValueError:
        syms_no_data.append(sym)


# %% codecell
#############################################################
low_wCounts = st_df[st_df['wCount'] < 1000].shape

def local_dates():
    """Get dict of missing local dates."""
    # Min date for stocktwits trending data
    st_date_min = datetime.date(2021, 2, 19)
    date_list = getDate.busDays(st_date_min)
    dl_ser = pd.Series(date_list)

    # Get fpaths for all local syms
    local_stock_data = glob.glob(f"{baseDir().path}/StockEOD/{date.today().year}/*/**")
    local_stock_data = sorted(local_stock_data)

    local_syms = []  # Create an empty list
    for st in local_stock_data:  # Split strings and store symbol names
        local_syms.append(st.split('_')[1][:-3])

    syms_dict = {}
    syms_dict_to_get = {}
    syms_not_get = []
    for st, path in zip(local_syms, local_stock_data):
        syms_dict[st] = pd.read_json(path, compression='gzip')
        # syms_dict[st]['date'] = pd.to_datetime(syms_dict[st]['date'], unit='ms')
        try:
            syms_dict_to_get[st] = dl_ser[~dl_ser.isin(syms_dict[st]['date'])]
        except KeyError:
            syms_not_get.append(st)

    ld_dict = {}
    ld_dict['local_syms'] = local_syms
    ld_dict['syms_not_get'] = syms_not_get
    ld_dict['syms_dict_to_get'] = syms_dict_to_get
    ld_dict['syms_dict'] = syms_dict
    return ld_dict

ld_dict = local_dates()

# Get all the syms that are not saved locally
not_local_syms = [sym for sym in sym_list if sym not in local_syms]

# %% codecell
#############################################################
syms_dict['AAL'].dtypes
syms_dict_to_get.keys()
syms_dict_to_get['AAL']

syms_dict[st]

# %% codecell
#############################################################

def convert_json_to_gz():
    # Get fpaths for all local syms
    local_stock_data = glob.glob(f"{baseDir().path}/StockEOD/{date.today().year}/*/**")
    local_stock_data = sorted(local_stock_data)

    local_syms = []  # Create an empty list
    for st in local_stock_data:  # Split strings and store symbol names
        local_syms.append(st.split('_')[1])

    local_syms_dict = {}
    for sym, path in zip(local_syms, local_stock_data):
        local_syms_dict[sym] = pd.read_json(path)
        local_syms_dict[sym].drop_duplicates(subset=['date'], inplace=True)

    local_stock_gz = deepcopy(local_stock_data)
    local_stock_gz = sorted([f"{path}.gz" for path in local_stock_gz])

    for sym, path in zip(local_syms_dict, local_stock_gz):
        local_syms_dict[sym].to_json(path, compression='gzip')

    # Remove original json files
    for st in local_stock_data:
        if '.gz' not in st:
            os.remove(st)

# %% codecell
#############################################################









daily_mean = st_df.groupby(by=['symbol', 'date']).mean()
daily_mean.reset_index(inplace=True)
daily_mean.dropna(inplace=True)

daily_mean['symbol'].value_counts(ascending=False)

daily_mean[daily_mean['symbol'] == 'KMPH']










# %% codecell
#############################################################
