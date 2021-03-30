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

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

#from data_collect.iex_routines import iexClose
#importlib.reload(sys.modules['data_collect.iex_routines'])
#from data_collect.iex_routines import iexClose

from multiuse.help_class import baseDir, dataTypes, getDate, local_dates
from data_collect.iex_class import readData, urlData
# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################

iex_eod = serverAPI('iex_quotes_raw')
iex_df = iex_eod.df.T.copy(deep=True)


iex_get = requests.get('https://algotrading.ventures/api/v1/prices/eod/all')
iex_json = iex_get.json()

iex_get

iex_get_df = iex_get


all_df = pd.DataFrame()
for key in iex_json.keys():
    try:
        df = pd.DataFrame(iex_json[key])
        all_df = pd.concat([all_df, df])
    except ValueError:
        print(key)



for ix in iex_df.index:
    df = pd.DataFrame(iex_df.iloc[0, :])
    break

iex_df.reset_index(drop=True, inplace=True)

iex_df.info(memory_usage='deep')

iex_times = pd.to_datetime(iex_df['closeTime'], unit='ms')
iex_times.value_counts(ascending=False).head(500)

iex_times.max()

iex_df.head(10)

# %% codecell
##################################

iex_close = iexClose()

len(iex_close.get.content)
iex_close.url

a_today = pd.DataFrame(iex_close.get.json(), index=range(1))
iex_close.get.content

year = date.today().year
sym = 'A'

fpath_base = f"{baseDir().path}/iex_eod_quotes"
fpath = f"{fpath_base}/{year}/{sym.lower()[0]}/_{sym}.gz"


exist = pd.DataFrame([pd.read_json(fpath, compression='gzip', typ='series')])
exist
exist.head(10)

# %% codecell
##################################

iex_close = iexClose()


# %% codecell
##################################

# GET /stock/{symbol}/quote/{field}




# %% codecell
##################################
