"""Workbook for calendar of company filings."""
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
    from scripts.dev.missing_data.missing_sec_masteridx import get_missing_sec_master_idx
    from scripts.dev.missing_data.missing_dates import get_missing_dates
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.sec_routines import secMasterIdx
    from missing_data.missing_sec_masteridx import get_missing_sec_master_idx
    from missing_data.missing_dates import get_missing_dates
    from multiuse.help_class import getDate, baseDir, write_to_parquet
    from api import serverAPI


# %% codecell
serverAPI('redo', val='get_missing_sec_master_idx')
serverAPI('redo', val='combine_all_sec_masters')
# %% codecell


# %% codecell


sma_api = serverAPI('sec_master_all')
sma_df = sma_api.df.copy()
sma_df['date'] = pd.to_datetime(sma_df['date'], unit='ms')

sma_df.shape
sma_df.head()
# %% codecell




# The only thing I want here is a reference sheet of 10q filings
"""
Form 10-q - quarterly financial statement
Form nt-10-q - firm's inability to file 10-q in a timely manner
    - Most common reason is a merger or acquisition
    - Filed within 45 days following end of each of the first 3 fiscal quarters
Form 10-d: asset backed issuer distribution form
    - Used for notification of interest, dividends and capital distributions

Form 10-k - annual financial statements (not an annual report)
Form 10-k/a - ammendment to form 10-k
"""
# sma_df['date'].unique()
# %% codecell

def company_10qs_ref():
    """Get ref data for company 10qs (quarterly filings)."""
    sma_api = serverAPI('sec_master_all')
    sma_df = sma_api.df.copy()
    sma_df['date'] = pd.to_datetime(sma_df['date'], unit='ms')

    forms_list = sma_df['Form Type'].value_counts().index
    # form_10 = [f for f in forms_list if '10' in str(f)]
    form_10qs = [f for f in forms_list if '10-Q' in str(f)]
    f10q_df = sma_df[sma_df['Form Type'].isin(form_10qs)].copy()

    all_syms = serverAPI('all_symbols').df
    all_syms.drop(columns=['date'], inplace=True)

    min_cik_len = all_syms['cik'].str.len().min()
    if min_cik_len < 10:
        print('Not all CIKs are 10 digits long')

    f10q_df.rename(columns={'name': 'sec_name'}, inplace=True)
    comb_df = pd.merge(f10q_df, all_syms, on=['cik'])

    tenq_df = comb_df[comb_df['Form Type'] == '10-Q'].copy()
    tenq_df.drop_duplicates(subset=['date', 'cik'], inplace=True)

    cols_to_keep = ['cik', 'symbol', 'date', 'name', 'Form Type']
    tenq_df = tenq_df[cols_to_keep]

    path = Path(baseDir().path, 'ref_data', 'symbol_10q_ref.parquet')
    write_to_parquet(tenq_df, path)

# %% codecell

company_10qs_ref()
# tenq_df - turn it into one-hot encoding for historical dates
# Or can say "days until next 10-q"
path = Path(baseDir().path, 'ref_data', 'symbol_10q_ref.parquet')

# Days since - days until - then subtract the index


tenq_df


# %% codecell

from missing_data.missing_dates import get_missing_dates
importlib.reload(sys.modules['missing_data.missing_dates'])

gmd = get_missing_dates(sma_df)
# This is interesting - I'd have to go back into the local fpaths to
# see which dates weren't collected


# %% codecell


# %% codecell

# I should just make this part of the function, honestly.
# I've had to do this same operation so many times
from missing_data.missing_sec_masteridx import get_missing_sec_master_idx
importlib.reload(sys.modules['missing_data.missing_sec_masteridx'])
from missing_data.missing_sec_masteridx import get_missing_sec_master_idx


# %% codecell






# %% codecell
