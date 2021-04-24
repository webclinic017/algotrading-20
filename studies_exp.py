
# %% codecell
#############################################
import glob
from datetime import date

import pandas as pd

from api import serverAPI

from studies.studies import regStudies
from studies.drawings import makeDrawings
from multiuse.help_class import baseDir, getDate, dataTypes

import importlib
import sys
importlib.reload(sys.modules['multiuse.help_class'])


# Display max 50 columns
pd.set_option('display.max_columns', 100)
# Display maximum rows
pd.set_option('display.max_rows', 500)
# %% codecell
#############################################

base_path = f"{baseDir().path}/StockEOD/{date.today().year}"
globs = glob.glob(f"{base_path}/*/**")
globs

for path in globs:
    df = pd.read_json(path, compression='gzip')
    break

df = regStudies(df).df
df = makeDrawings(df).df

df['localMin_5'].value_counts()

df['localMin_10'].value_counts()

df.head(10)

df.shape
# %% codecell
#############################################

df = serverAPI('iex_quotes_raw').df
df.shape

iex_df = dataTypes(df).df

# 27 mbs with data type adjustments
iex_df.info(memory_usage='deep')
# 154 mbs without data type adjustments
df.info(memory_usage='deep')
import numpy as np
np.finfo('float32')
np.finfo('float16')

np.finfo('float16').max
np.finfo('float32').max
# %% codecell
#############################################
df['closeTime'].value_counts()
iex_df['closeDate'] = pd.to_datetime(iex_df['closeTime'], unit='ms')
iex_df['closeDate'].value_counts()

iex_df.sort_values(by=['latestTime'], ascending=True).head(1)
iex_df.sort_values(by=['ytdChange'], ascending=False).head(1)








# %% codecell
#############################################
