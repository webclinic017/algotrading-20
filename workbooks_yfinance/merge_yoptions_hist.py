"""Merge yoptions with historical data."""
# %% codecell

from pathlib import Path

import pandas as pd
import numpy as np
from tqdm import tqdm
# %% codecell

from api import serverAPI

from multiuse.help_class import baseDir, getDate, df_create_bins, help_print_arg, dataTypes
from data_collect.yfinance_funcs import clean_yfinance_options, get_cboe_ref, return_yoptions_temp_all, yoptions_combine_last
# %% codecell


def analyze_iex_ytd():
    """Analyze iex historical data for this year."""
    df_prices_get = serverAPI('stock_close_prices').df
    df_prices = df_prices_get.copy()
    df_prices['date'] = pd.to_datetime(df_prices['date'], unit='ms')

    dt_max = df_prices['date'].max().date()
    path = Path(baseDir().path, 'StockEOD/combined', f"_{dt_max}.parquet")
    df_prices.to_parquet(path)

    df_2021 = df_prices[df_prices['date'].dt.year >= 2021].copy()
    return df_2021


def get_clean_yoptions():
    """Get and clean yoptions data."""
    df_cleaned = None
    df_temp = serverAPI('yoptions_all').df

    try:
        df_cleaned = clean_yfinance_options(df_temp=df_temp, refresh=True).copy()
    except Exception as e:
        print(str(e))

    df_cleaned['date'] = (pd.to_datetime(df_cleaned['lastTradeDate'], format='%Y-%m-%d')
                            .dt.normalize())
    df_cleaned['symbol'] = df_cleaned['Underlying']

    return df_cleaned


def add_perc_change_columns(df_prices=False, df_cleaned=False, refresh=False):
    """Use historical 2021 data from iex in prep to merge."""
    perc_path = Path(baseDir().path, 'StockEOD/combined', "_2021_yprices_percs.parquet")
    # If perc path exists, return that file instead of running whole analysis
    if perc_path.exists() and not refresh:
        df_y = pd.read_parquet(perc_path)
        max_date = df_y.index.get_level_values('date').max()
        print(f"Most recent date for historical data is: {max_date}")
        return df_y

    df_yprices = df_prices[df_prices['date'] >= df_cleaned['date'].min()].copy()
    cols_to_keep = (['date', 'symbol', 'fOpen', 'fClose', 'fHigh', 'fLow',
                     'fVolume', 'change', 'changePercent', 'changeOverTime',
                     'marketChangeOverTime'])

    df_y = df_yprices[cols_to_keep].copy()
    path = Path(baseDir().path, 'StockEOD/combined', "_2021_yprices.parquet")
    df_yprices.to_parquet(path)

    df_y = (df_y.dropna(subset=['date', 'symbol'])
                .drop_duplicates(subset=['date', 'symbol'])
                .set_index(['date', 'symbol'])
                .sort_index(level=['date', 'symbol']))

    df_y['fRange'] = (df_y['fHigh'] - df_y['fLow']).round(2)
    syms = df_y.index.get_level_values('symbol').unique().tolist()
    idx = pd.IndexSlice

    cols_cperc_change = ['c_perc1', 'c_perc2', 'c_perc3', 'c_perc5', 'c_perc7']
    cols_operc_change = ['o_perc1', 'o_perc2', 'o_perc3', 'o_perc5', 'o_perc7']
    all_perc_cols = cols_cperc_change + cols_operc_change
    df_y[all_perc_cols] = 0

    for sym in tqdm(syms):
        df_sub = df_y[df_y.index.get_level_values('symbol') == sym].copy()
        for col in all_perc_cols:
            df_sub[col] = -df_sub['fClose'].pct_change(periods=-int(col[-1]))
        df_y.loc[idx[df_sub.index, all_perc_cols]] = df_sub[all_perc_cols]

    perc_path = Path(baseDir().path, 'StockEOD/combined', "_2021_yprices_percs.parquet")

    df_y = dataTypes(df_y, parquet=True).df
    df_y.to_parquet(perc_path)

    return df_y


# %% codecell


def get_clean_all_st_data():
    """Get all stocktwits data, clean it all too."""

    st_data = serverAPI('st_trend_all').df
    st_data = dataTypes(st_data).df
    st_data['timestamp'] = pd.to_datetime(st_data['timestamp'], unit='ms')
    st_data['date'] = st_data['timestamp'].dt.normalize()

    st_na = False
    try:
        st_group = st_data.groupby(by=['symbol', 'date']).count()
        st_na = st_group.dropna()
    except Exception as e:
        print(str(e))

    st_counts = (st_na.reset_index()
                      .drop(columns=['watchlist_count', 'timestamp'])
                      .rename(columns={'id': 'count'}))
    st_all = pd.merge(st_data, st_counts, on=['symbol', 'date'])
    st_all.rename(columns={'timestamp': 'st_time'}, inplace=True)

    st_all = dataTypes(st_all).df

    return st_all


# %% codecell
