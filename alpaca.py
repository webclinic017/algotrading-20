"""Alpaca trading."""
# %% codecell
####################################
import os
from dotenv import load_dotenv
import sys
import importlib

import requests
from requests import Session, Request
import pandas as pd
import numpy as np
import json

from datetime import datetime, date

from api import serverAPI

from multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg, rate_limit
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg, rate_limit

from multiuse.create_file_struct import make_hist_prices_dir

from data_collect.apca_routines import apca_params, ApcaSymbols, ApcaHist
importlib.reload(sys.modules['data_collect.apca_routines'])
from data_collect.apca_routines import apca_params, ApcaSymbols, ApcaHist

# %% codecell
####################################
import time

apca_syms = ApcaSymbols().df
apca_syms_active = apca_syms[apca_syms['status'] == 'active'].copy(deep=True)
apca_syms_active.shape
apca_syms.shape

kwargs = {'sym_list': apca_syms_active['symbol'].tolist()}
rate_limit(ApcaHist, testing=True, **kwargs)

capu_sym = 'CAP.U'


apca_syms['symbol'].tolist()
apca_syms.head(10)





apca_syms = ApcaSymbols()


ref_df['class'].value_counts()
ref_df['status'].value_counts()
all_symbols = serverAPI('all_symbols').df
ref_df_not_in = ref_df[~ref_df['symbol'].isin(all_symbols['symbol'].tolist())]
ref_df_not_in['symbol'].shape

syms_not_in_apca = all_symbols[~all_symbols['symbol'].isin(ref_df['symbol'].tolist())]
syms_not_in_apca['type'].value_counts()

# APCA has all symbols in iex_symbols except for warrants and structured products

ref_df.head(10)





headers, base_url = apca_params(markets=False)
url = f"{base_url}/stocks/OCGN/bars"


bus_days = getDate().get_bus_days(this_year=True)
iex_start_date = bus_days.min()['date'].date()
iex_end_date = getDate.query('iex_eod')
iex_start = getDate.date_to_rfc(iex_start_date)
iex_end = getDate.date_to_rfc(iex_end_date)


params = {'start': iex_start, 'end': iex_end, 'limit':10000, 'timeframe': '1Day'}
get = requests.get(url, headers=headers, params=params)

get_json = get.json()
df = pd.DataFrame(get_json['bars'])
df['date'] = getDate.rfc_to_date(df['t'])

df.head(10)
df['date'] = getDate.rfc_to_date(df['t'])

base_path_dir = f"{baseDir().path}/apcaBars"
make_hist_prices_dir(base_path_dir)



class ApcaData():
    """Helper class for getting alpaca data."""
    df, next_page_token = None, None

    def __init__(self, which, symbol, start, end, timeframe=None):
        headers, base_url = apca_params()
        url = self.construct_url(which, base_url, symbol)
        params = self.construct_params(start, end)


    @classmethod
    def construct_url(cls, which, base_url, symbol):
        """Construct url for request."""
        url = ''

        if which == 'quotes':
            url = f"{base_url}/stocks/{symbol}/quotes"

        return url

    @classmethod
    def construct_params(cls, start, end):
        """Construct parameters used for request."""
        params = {'start': start, 'end': end, 'limit': 10000}

        return params

    @classmethod
    def get_data(cls, self, url, headers, params, which):
        """Get data from alpaca and store under self.df."""
        get = requests.get(url, headers=headers, params=params)
        if get.status_code < 400:
            get_json = get.json()
            self.df = pd.DataFrame(get_json[which])
            self.next_page_token = get_json['next_page_token']



"""
ax = ask exchange
ap = ask price
as = ask size
bx = bid exchange
bp = bid price
bs = bid size
c = conditons
"""

ocgn_df = pd.DataFrame(get_json['quotes'])

ocgn_df.head(10)

# %% codecell
####################################
