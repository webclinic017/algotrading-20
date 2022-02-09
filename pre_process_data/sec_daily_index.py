"""Get the sec daily index file from server."""
# %% codecell
from pathlib import Path
from datetime import date
import time

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet
    from scripts.dev.api import serverAPI
    from scripts.dev.data_collect.sec_routines import sec_sym_list
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_parquet
    from api import serverAPI
    from data_collect.sec_routines import sec_sym_list

# %% codecell


def get_collect_prep_sec_data(df=False):
    """Get SEC master index file for each day since 2021."""
    sma_api = serverAPI('sec_master_all')
    sma_df = sma_api.df.copy()
    sma_df['date'] = pd.to_datetime(sma_df['date'], unit='ms')
    sma_df['cik'] = sma_df['cik'].astype('category')

    dt = date(2021, 1, 1)
    sma_df = sma_df[sma_df['date'].dt.date > dt].copy()

    tenq = ['10-Q', 'NT 10-Q', '10-Q/A']
    tenk = ['10-K', '10-K/A', 'NT 10-K']
    eightk = ['8-K', '8-K/A']

    sma_df['tenq'] = np.where(
        sma_df['Form Type'].isin(tenq),
        1, 0
    )
    sma_df['tenk'] = np.where(
        sma_df['Form Type'].isin(tenk),
        1, 0
    )
    sma_df['eightk'] = np.where(
        sma_df['Form Type'].isin(eightk),
        1, 0
    )

    # Get symbol reference data from sec
    sec_syms = None
    path = Path(baseDir().path, 'tickers', 'symbol_list', 'sec_syms.parquet')
    if path.exists():
        sec_syms = pd.read_parquet(path)
    else:
        sec_sym_list()
        time.sleep(2)
        sec_syms = pd.read_parquet(path)

    sec_syms.rename(columns={'cik_str': 'cik'}, inplace=True)

    cols_to_drop = ['name', 'File Name']
    sma_df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    df_form_match = (sma_df[sma_df[['tenq', 'tenk', 'eightk']]
                     .isin([1]).any(axis=1)].copy())

    sec_df = pd.merge(sec_syms, df_form_match, on='cik', how='inner')
    sec_df.drop(columns=['cik', 'title', 'Form Type'], inplace=True)
    sec_df.rename(columns={'ticker': 'symbol'}, inplace=True)

    sec_df = (sec_df.groupby(by=['symbol', 'date'], as_index=False)
                    .sum().copy())

    if isinstance(df, pd.DataFrame):
        sec_df = pd.merge(df, sec_df, on=['symbol', 'date'], how='left')

    return sec_df


# %% codecell
