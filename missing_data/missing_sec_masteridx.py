"""Get missing sec master idx file."""
# %% codecell
from pathlib import Path
from time import sleep
import importlib
import sys
from tqdm import tqdm
import pandas as pd
import numpy as np

try:
    from scripts.dev.data_collect.sec_routines import secMasterIdx
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.sec_routines import secMasterIdx
    from api import serverAPI
    from multiuse.help_class import getDate

# %% codecell

def get_missing_sec_master_idx(sma_df=False):
    """Get missing sec reference data files."""
    # sma_df is the master index file of all dates
    if not isinstance(sma_df, pd.DataFrame):
        sma_df = serverAPI('sec_master_all').df
        sma_df['date'] = pd.to_datetime(sma_df['Date Filed'], unit='ms')

    bus_days = getDate.get_bus_days(this_year=True)
    dt = getDate.query('iex_eod')
    bus_days = bus_days[bus_days['date'].dt.date <= dt].copy()

    dts_missing = bus_days[~bus_days['date'].isin(sma_df['date'].unique().tolist())].copy()
    dts_missing['dt_format'] = dts_missing['date'].dt.strftime('%Y%m%d')

    for dt in tqdm(dts_missing['dt_format']):
        smi = secMasterIdx(hist_date=dt)
        sleep(.5)

# %% codecell
