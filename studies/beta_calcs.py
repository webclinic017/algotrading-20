"""Beta calculations."""
# %% codecell
from pathlib import Path

import pandas as pd
import numpy as np
from tqdm import tqdm
import yfinance as yf

try:
    from scripts.dev.workbooks.fib_funcs import read_clean_combined_all
except ModuleNotFoundError:
    from workbooks.fib_funcs import read_clean_combined_all

# %% codecell


def get_beta_vals(df_all=None):
    """Calculate beta values for stocks against SPY."""
    if not isinstance(df_all, pd.DataFrame):
        df_all = read_clean_combined_all(local=False)
        df_all = df_all[df_all['date'] >= '2021']

    mkt_data = (yf.download(tickers=['SPY'], period='ytd',
                            interval='1d', auto_adjust=True))

    cols_to_rename = ({'Date': 'date', 'Open': 'fOpen', 'High': 'fHigh',
                       'Low': 'fLow', 'Close': 'mktClose', 'Volume': 'fVolume'})

    df_spy = mkt_data.reset_index().rename(columns=cols_to_rename)
    df_spy.insert(1, 'symbol', 'SPY')
    df_spy['fChangeP'] = df_spy['mktClose'].pct_change(limit=1)
    df_spy['fChangeP'].fillna(method='bfill', inplace=True)

    df_all_pre = df_all[['date', 'symbol', 'fClose']].set_index(['date'])
    df_all_pivot = df_all.pivot('date', 'symbol', 'fClose').reset_index()
    df_spy_pivot = df_spy.pivot('date', 'symbol', 'mktClose').reset_index()

    for col in tqdm(df_all_pivot.columns[1:]):
        df_spy_pivot[col] = df_spy_pivot['SPY']

    df_spy_pivot.drop(columns=['SPY'], inplace=True)

    s_mkt_corr = df_all_pivot.corrwith(df_spy_pivot, axis=0, method='pearson')

    df_all_ret_piv = df_all.pivot('date', 'symbol', 'fChangeP').reset_index().drop(columns=['date'])
    df_spy_ret_piv = df_spy.pivot('date', 'symbol', 'fChangeP').reset_index().drop(columns=['date'])
    s_all_std = df_all_ret_piv.std(axis=0)
    s_mkt_std = df_spy_ret_piv.std(axis=0)

    sym_list = df_all['symbol'].unique().tolist()

    beta_pairs = []
    for sym in tqdm(sym_list):
        beta = (s_mkt_corr.loc[sym] * (s_all_std.loc[sym] / s_mkt_std.iloc[0]))
        beta_pairs.append((sym, beta))

    beta_df = pd.DataFrame(beta_pairs, columns=['symbol', 'beta'])

    return beta_df

# %% codecell
