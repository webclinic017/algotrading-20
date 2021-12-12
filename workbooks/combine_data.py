"""Local copy of remote combine file."""
# %% codecell
import os
import sys
from pathlib import Path
from io import BytesIO
import importlib
from tqdm import tqdm

import datetime

import requests
import pandas as pd
import numpy as np


try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripst.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions
    from multiuse.path_helpers import get_most_recent_fpath
    from api import serverAPI

# %% codecell
bpath = Path(baseDir().path, 'StockEOD/2021')
fpath_list = list(bpath.glob('**/*.parquet'))

df_list = []

for fpath in tqdm(fpath_list):
    try:
        df_list.append(pd.read_parquet(fpath))
    except Exception as e:
        print(str(e))

dt = getDate.query('iex_eod')
fpath = Path(baseDir().path, 'StockEOD/combined_all', f"_{dt}.parquet")

df_all = pd.concat(df_list).reset_index(drop=True)
df_all['date'] = pd.to_datetime(df_all['date'])

df_all = dataTypes(df_all, parquet=True).df
df_all.to_parquet(fpath)

# %% codecell
sym_list = df_2021['symbol'].unique().tolist()
bpath = Path(baseDir().path, 'StockEOD/2021')

for sym in tqdm(sym_list):
    df_mod = df_all[df_all['symbol'] == sym]
    fpath = Path(bpath, sym.lower()[0], f"_{sym}.parquet")
    write_to_parquet(df_mod, fpath)


# %% codecell

def combine_daily_stock_eod(parq=True, gz=False):
    """Combine all iex hist daily data."""
    dt = getDate.query('iex_previous')
    base_path = Path(baseDir().path, 'StockEOD')

    parqs_to_read = list(Path(base_path, str(dt.year)).glob('**/*.parquet'))
    gzs_to_read = list(Path(base_path, str(dt.year)).glob('**/*.gz'))
    eod_comb_list = []
    fpath_errors, associated_errors = [], []
    cols_to_read = ['date', 'symbol', 'fOpen', 'fHigh', 'fLow', 'fClose', 'fVolume']

    if parq:
        for f in tqdm(parqs_to_read):
            try:
                if f.exists():
                    eod_comb_list.append(pd.read_parquet(f, columns=cols_to_read))
            except Exception as e:
                fpath_errors.append(f)
                associated_errors.append(str(e))
    if gz:
        for f in tqdm(gzs_to_read):
            try:
                if f.exists():
                    eod_comb_list.append(pd.read_json(f, compression='gzip'))
            except Exception as e:
                fpath_errors.append(f)
                associated_errors.append(str(e))

    df_eod_comb = pd.concat(eod_comb_list).reset_index(drop=True)
    df_eod = dataTypes(df_eod_comb, parquet=True).df

    # Create local fpath for gz and parquet
    cb_all_path = Path(base_path, 'combined_all')

    bpath = Path(cb_all_path, f"_{str(dt)}.parquet")
    write_to_parquet(df_eod, bpath)

    dt_max = df_eod['date'].max()
    df_today = df_eod[df_eod['date'] == dt_max].reset_index(drop=True)
    comb_path = Path(base_path, 'combined', f"_{str(dt_max)}.parquet")
    write_to_parquet(df_today, comb_path)

# %% codecell
