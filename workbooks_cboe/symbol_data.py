"""Analyzing cboe symbol options data."""
# %% codecell
from pathlib import Path
from datetime import datetime, date

import requests
import pandas as pd
import numpy as np

from io import BytesIO

try:
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import getDate
    from api import serverAPI

# %% codecell


url = 'https://www.cboe.com/us/options/market_statistics/symbol_data/csv/?mkt=cone'
get = requests.get(url)

df = None
try:
    df = pd.read_csv(BytesIO(get.content))
    df.columns = [col.lower() for col in df.columns]
except Exception as e:
    print(e)

# %% codecell
df.head()

# Cool, so there are 2,976 different unique symbols on a friday
# I wonder what the cutoff is for this. I can probably request every 10 minutes
# Then I'd have to subtract from the previous 10 minute dataframe,
# which is where this stuff gets complicated

# What I can do is combine with the previous days data for open interest, avg volume,
# Then analyze in real time without paying a fortune.

# Can also analyze based on routed, sign of unusual volume
# Exclude etfs

all_syms = serverAPI('all_symbols').df
all_cs = all_syms[all_syms['type'].isin(['cs', 'ad'])]

df_eq = df[df['symbol'].isin(all_cs['symbol'].tolist())].copy()

# This is where the iex stats comes in handy
sc_api = serverAPI('stats_combined')
df_stats = sc_api.df
# Temporary measure to deal with the lack of a date column
df_stats.drop_duplicates(subset=['companyName'], inplace=True)

# Okay so we'll drop duplicates based on the company name
df_stats_ref = pd.merge(all_syms, df_stats, left_on=['name'], right_on=['companyName'])
df_stats_ref['marketcap'] = (df_stats_ref['marketcap'].div(1000000, fill_value=0)).astype('int64')
df_stats_ref.drop(columns=['date'], inplace=True)

# Combine df_stats_ref with df_eq
df_eq = pd.merge(df_eq, df_stats_ref, on=['symbol'])

# If data is current
dt_close = getDate.query('iex_close')
if dt_close == date.today():
    df_eq['time'] = pd.Timestamp.now()

# %% codecell
df_eq.info()
df_stats_ref.columns
cols_to_keep = df.columns.tolist() + ['marketcap', 'float', 'beta', 'day5ChangePercent', 'nextEarningsDate']

# If on Friday, we could exclude that Friday as may not be unusual volume

df_eq.sort_values(by=['routed'], ascending=False)[cols_to_keep].head(50)


df.sort_values(by=['Routed'], ascending=False).head(50)



df['Symbol'].unique().shape


df.info()

# %% codecell
