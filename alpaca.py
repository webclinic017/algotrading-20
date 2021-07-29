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

from multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg

from multiuse.create_file_struct import make_hist_prices_dir

# %% codecell
####################################



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





headers, base_url = apca_params(markets=True)
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



api = REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY)
aapl = api.get_quotes("AAPL")


bus_days = getDate().get_bus_days(this_year=True)
iex_start_date = bus_days.min()['date'].date()
iex_end_date = getDate.query('iex_eod')
iex_start_date
len(bus_days)

aapl = api.get_quotes("AAPL", start=str(iex_start_date), end=str(iex_end_date), limit=len(bus_days)).df
aapl.head(10)
aapl['conditions'].value_counts()

# %% codecell
####################################


load_dotenv()
redirect_uri = "http://localhost:8080"
random_string = os.environ.get("alpaca_state")
client_id = os.environ.get("alpaca_client_id")
client_secret = os.environ.get("alpaca_client_secret")

url_1 = 'https://app.alpaca.markets/oauth/authorize?'
url_2 = f"response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
url_3 = f"&state={random_string}&scope=account:write%20trading"

full_url = f"{url_1}{url_2}{url_3}"
full_url_dumped = json.dumps(full_url)
random_string
from urllib.parse import urlencode, quote
url_dict = {'response_type': 'code', 'client_id': client_id, 'redirect_uri': redirect_uri, 'state': random_string}

url_body = urlencode(url_dict)
url_beginning = 'https://app.alpaca.markets/oauth/authorize?'
url_end = f"&scope=data"

url_all = f"{url_beginning}{url_body}{url_end}"
url_all



base_url = "https://api.alpaca.markets/v2"
url = f"{base_url}/assets"
load_dotenv()

alpaca_code = os.environ.get("alpaca_code")


post_url = "https://api.alpaca.markets/oauth/token"
post_params = {'grant_type': 'authorization_code', 'code': alpaca_code, 'client_id': client_id, 'client_secret': client_secret, 'redirect_uri': 'https://algotrading.ventures'}
post = requests.post(post_url, params=post_params)
post.status_code
post.content
post_params
client_id = os.environ.get("alpaca_client_id")
client_secret = os.environ.get("alpaca_client_secret")

payload = {"APCA-API-KEY-ID": client_id, "APCA-API-SECRET-KEY": client_secret}

get = requests.get(url, headers=payload)
get.status_code
get.content
payload
