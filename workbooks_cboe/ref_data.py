"""Cboe options reference data."""
# %% codecell
from pathlib import Path
from io import BytesIO

import dask.dataframe as dd
import pandas as pd
import numpy as np
import requests

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from multiuse.path_helpers import get_most_recent_fpath

# %% codecell


# %% codecell
# def cboe_clean_symbol_ref
bpath = Path(baseDir().path, 'ref_data/yoptions_ref/cboe_ref_raw')
fpath = get_most_recent_fpath(bpath)
cols_to_read = ['OSI Symbol', 'Underlying']

df = dd.read_parquet(fpath, columns=cols_to_read)

df['OSI Symbol'] = df['OSI Symbol'].str.replace(' ', '')
df['Underlying'] = df['Underlying'].str.replace('.', '', regex=False)

df = df[df['Underlying'] != 'C']

df['sym_suf'] = df.apply(lambda row: row['OSI Symbol'].replace(row['Underlying'], ''), axis=1, meta=('sym_suf', 'object'))

df = df.assign(suf_temp=df['sym_suf'].str.replace(' ', '').str.replace('C', ' ').str.replace('P', ' ').str.split(' '))
df['sym_suf'] = df.apply(lambda row: list(filter(None, row['sym_suf'])), axis=1, meta=('sym_suf', 'object'))
df['expDate'] = df.apply(lambda row: row['suf_temp'][0], axis=1, meta=('expDate', 'object'))
df['expDate'] = dd.to_datetime(df['expDate'], format='%y%m%d', errors='coerce')

df_con = df.compute()

# %% codecell

# df['expDate'] = df.apply(lambda row: row['suf_temp'][0], axis=1, meta=('expDate', 'object'))
# df['expDate'] = dd.to_datetime(df['expDate'], format='%y%m%d')

# %% codecell
df_samp = df_con.sample(n=500000)

df_samp.info()

df_less = df_samp.sample(n=100)
df_less['OSI Symbol'].str.rsplit(r"P|C", expand=True)


df_samp['strike_temp'] = df_samp['OSI Symbol'].str.rsplit(r"P|C", expand=True)

df_samp['strike_temp_len'] = df_samp['strike_temp'].str.len()

df_samp['strike_temp_len'].value_counts()
df_samp[df_samp['strike_temp'].str.contains(string.ascii_letters)]

import string

df_samp['stri']

df_samp[df_samp['expDate'].isna()]


df_con

df_con[df_con['Underlying'] == 'BRKB']
df_con[df_con['Underlying'] == 'BRKB']

df.info()

# %% codecell



















# %% codecell
