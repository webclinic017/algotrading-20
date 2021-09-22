"""Workbook for getting options data from IEX Cloud."""

# %% codecell
########################################################
import os
from datetime import date
import string
import importlib
import sys

import pandas as pd
import requests
from dotenv import load_dotenv

from multiuse.help_class import baseDir, dataTypes, getDate, RWJsonDicts
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import baseDir, dataTypes, getDate, RWJsonDicts

from api import serverAPI

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
########################################################
import glob
import threading
base_dir = baseDir().path
fpath = f"{base_dir}/derivatives/iex_symref/**"
paths = glob.glob(fpath, recursive=True)


view_symref = IexOptionSymref('VIEW')

all_syms = serverAPI('all_symbols').df
all_cs = all_syms[all_syms['type'] == 'cs']
all_cs.shape
all_cs_sym = all_cs['symbol'].tolist()

for sym in all_cs_sym:
    th = threading.Thread(target=IexOptionSymref, args=(sym,))
    th.start()

all_syms.head(10)

def iex_options_symbol_ref():
    """Add tasks to queue to execute."""
    syms_fpath = f"{base_dir}/tickers/all_symbols.gz"
    all_syms = pd.read_json(syms_fpath, compression='gzip')

    all_cs = all_syms[all_syms['type'] == 'cs'].tolist()
    for sym in all_cs:



load_dotenv()
base_url = os.environ.get("base_url")
payload = {'token': os.environ.get("iex_publish_api")}
url = f"{base_url}/ref-data/options/symbols"
get = requests.get(url, params=payload)
get_json = get.json()

np_json = get_json.values()


# Dictionary of expiration dates needs to be written to local file.
import json
fpath = f"{base_dir}/derivatives/iex_symref/expos.json"

# %% codecell
########################################################

iex_options_symref = serverAPI('redo', val='get_iex_symbol_ref')

data = RWJsonDicts(local_dict='iex_options_symref').data

# 1000 per symbol per expo date.
len(data['AAPL'])

29000 * 1000

50000000 / 30 / 21

all_syms = serverAPI('all_symbols').df

all_etfs = all_syms[all_syms['type'] == 'et']['symbol'].tolist()

non_etfs = []
non_etfs_counts = 0
for key, val in data.items():
    if key not in all_etfs:
        non_etfs.append(key)
        non_etfs_counts += len(val)

print(non_etfs)
print(non_etfs_counts)


25000 * 1000
all_syms['type'].value_counts()



# %% codecell
########################################################


def count_exp_dates(data):
    """Count the number of option expo dates."""
    exp_counts = 0
    for key, val in data.items():
        exp_counts += len(val)

    print(f"There are {len(data.keys())} unique symbols.")
    print()
    print(f"There are {exp_counts} expo dates.")


# %% codecell
########################################################
