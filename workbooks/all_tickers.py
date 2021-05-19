"""
The purpose of this file is to examine all the ticker data coming from IEX.

OTC data is separate from all_symbols

# Pink sheets is lowest tier
# OTCQB is the middle tier, the "Venture Market". Early stage and developing
    US and International companies net yet able to qualify for OTCQX
# OTCQX is the highest tier. Subject to SEC regulations.
"""

# %% codecell
################################################
import os
import json
from io import BytesIO, StringIO

import glob
import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

import datetime
from datetime import timedelta, date

from nested_lookup import nested_lookup

from api import serverAPI

# Display maximum rows
pd.set_option('display.max_rows', 500)
# Display max columns
pd.set_option('display.max_columns', None)

# %% codecell
################################################

def read_symbols(which):
    """Read ticker symbol data."""
    # Which can be 'all_symbols', 'otc'
    base_dir = f"{Path(os.getcwd()).parents[0]}/data"
    symbols_path = f"/tickers/{which}"

    try:
        symbols = json.load(StringIO(Path(f"{base_dir}{symbols_path}").read_bytes().decode("utf-8")))
        symbols = pd.DataFrame(symbols)
    except FileNotFoundError:
        choices = glob.glob(f"{base_dir}/tickers/*")
        symbols = f"Nothing found. Available choices are {choices}"
        print(symbols)

    return symbols

all_symbols = read_symbols('all_symbols')
otc = read_symbols('otc')

sectors = read_symbols('sector_lists')
sectors
# %% codecell
################################################
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
################################################


not_otc = all_symbols[~all_symbols.isin(otc)]
not_otc.shape

otc.shape
# %% codecell
################################################

all_symbols = serverAPI('all_symbols').df
all_symbols

# %% codecell
################################################

common = all_symbols[all_symbols['type'].isin(['cs'])]
common.shape

warrants = all_symbols[all_symbols['type'].isin(['wt'])]
warrants.shape


# %% codecell
################################################
warrants.head(10)


# %% codecell
################################################

all_symbols.head(10)

print(type(all_symbols))


# %% codecell
################################################
otc_pink = otc[otc['exchangeName'].isin(['OTC PINK CURRENT', 'OTCQB MARKETPLACE', 'OTCQX MARKETPLACE'])]

otc_warrants = otc_pink[otc_pink['type'].isin(['wt'])]
otc_warrants.shape
otc_pink.shape

otc['exchangeName'].value_counts()


otc.head(10)
