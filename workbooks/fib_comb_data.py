"""Combine data to fibonacci analysis dataframes."""
# %% codecell
from pathlib import Path

import pandas as pd
import numpy as np


try:
    from scripts.dev.multiuse.help_class import baseDir, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate

# %% codecell


def add_sec_days_until_10q(df_all):
    """Add days until 10q filing for df_all historical prices."""
    sec_10q_path = Path(baseDir().path, 'ref_data', 'symbol_10q_ref.parquet')
    sec_ref = pd.read_parquet(sec_10q_path)
    sec_ref['filing'] = sec_ref['date']

    df_all = pd.merge(df_all, sec_ref[['symbol', 'date', 'filing']], on=['symbol', 'date'], how='left')
    df_all['filing'] = df_all['filing'].fillna(method='bfill')

    df_all['date_test'] = df_all['date'].dt.date
    df_all['filing_test'] = df_all['filing'].dt.date

    holidays_fpath = Path(baseDir().path, 'ref_data/holidays.parquet')
    holidays = pd.read_parquet(holidays_fpath)

    dt = getDate.query('sec_master')
    current_holidays = (holidays[(holidays['date'].dt.year >= dt.year) &
                                 (holidays['date'].dt.date <= dt)])
    hol_list = current_holidays['date'].dt.date.tolist()
    df_all['date_test'].isna().sum()
    df_all['filing_test'].isna().sum()

    df_all.dropna(subset=['filing'], inplace=True)

    df_mod = df_all[['date_test', 'filing_test']].copy()
    df_mod['days_until'] = df_mod.apply(lambda row: np.busday_count(row['date_test'], row['filing_test'], holidays=hol_list), axis=1)
    df_all['days_until'] = df_mod['days_until']

    df_all.drop(columns=['date_test', 'filing_test'], inplace=True)

    return df_all

# %% codecell
