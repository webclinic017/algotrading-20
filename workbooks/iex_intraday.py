"""IEX Intraday."""
# %% codecell
from pathlib import Path
import os

import pandas as pd
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from data_collect.iex_class import urlData
    from api import serverAPI

from importlib import reload
import sys
reload(sys.modules['multiuse.help_class'])

# %% codecell
pd.set_option('display.max_columns', 65)
pd.set_option('display.max_rows', 150)

# %% codecell
dt = getDate.query('iex_eod')
bpath = Path(baseDir().path, 'intraday', 'minute_1', str(dt.year))

all_syms = serverAPI('all_symbols').df
syms = all_syms['symbol'].unique()


# %% codecell

minute = 'minute_1'


def combine_all_intraday_data(minute='minute_1'):
    """Combine all intraday data, write to file."""
    dt = getDate.query('iex_eod')
    path = Path(baseDir().path, 'intraday', minute, str(dt.year))
    fpaths = list(path.glob('**/*.parquet'))

    df_list = []
    for fpath in fpaths:
        try:
            df_list.append(pd.read_parquet(fpath))
        except Exception as e:
            msg = f"fpath: {str(fpath)} reason: {str(e)}"
            help_print_arg(msg)

    df_all = pd.concat(df_list)
    fpre = f'combined_all/{minute}/'
    fsuf = f"{fpre}_{dt}.parquet"
    path_to_write = path.parent.parent.joinpath(fsuf)

    write_to_parquet(df_all, path_to_write)



# %% codecell
combine_all_intraday_data()


# %% codecell




# %% codecell




# %% codecell


# %% codecell
