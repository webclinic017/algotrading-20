"""
The purpose of this file is to examine all the ticker data coming from IEX.

OTC data is separate from all_symbols
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

# Display maximum rows
pd.set_option('display.max_rows', None)

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

# %% codecell
################################################

not_otc = all_symbols[~all_symbols.isin(otc)]
not_otc.shape

otc.shape
# %% codecell
################################################

all_symbols['exchangeName'].value_counts()


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
