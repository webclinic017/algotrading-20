"""Use ML to predict which fib max_row to pick."""
# %% codecell
import os
import sys
from pathlib import Path
from io import BytesIO
import importlib
from tqdm import tqdm

import datetime
from datetime import timedelta

import requests
import pandas as pd
import numpy as np

import yfinance as yf

try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripst.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.multiuse.symbol_ref_funcs import get_symbol_stats
    from scripts.dev.workbooks.fib_comb_data import add_sec_days_until_10q
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions
    from multiuse.path_helpers import get_most_recent_fpath
    from multiuse.symbol_ref_funcs import get_symbol_stats
    from workbooks.fib_comb_data import add_sec_days_until_10q
    from api import serverAPI

from workbooks.fib_funcs import read_clean_combined_all, get_rows, get_fib_dict
importlib.reload(sys.modules['workbooks.fib_funcs'])
from workbooks.fib_funcs import read_clean_combined_all, get_rows, get_fib_dict
pd.set_option('display.max_columns', 30)

# %% codecell
from sklearn.model_selection import TimeSeriesSplit

# %% codecell

df_all = read_clean_combined_all(local=False)
# %% codecell
# %% codecell
# Drop all of these beforehand
df_all['cumPerc'].fillna(value=0, inplace=True)

df_all['vol_avg_2m'].fillna(method='bfill', inplace=True)
group_2m_max_vol = df_all[['symbol', 'vol_avg_2m']].groupby(by=['symbol']).max()
syms_over_hundred = group_2m_max_vol[group_2m_max_vol['vol_avg_2m'] > 100000].index.tolist()

df_100 = df_all[df_all['symbol'].isin(syms_over_hundred)].copy()

bpath = Path(baseDir().path, 'studies/fibonacci')
df_100_path = bpath.joinpath('df_100.parquet')
write_to_parquet(df_100, df_100_path)
# %% codecell
#######################################################################

bpath = Path(baseDir().path, 'studies/fibonacci')
df_100_path = bpath.joinpath('df_100.parquet')
df_100 = pd.read_parquet(df_100_path)

fib_df_path = bpath.joinpath('fib_vals.parquet')
fib_df = pd.read_parquet(fib_df_path)

path = Path(baseDir().path, 'studies/fibonacci', 'mrow_options.parquet')
mrow_df = pd.read_parquet(path)

cols_to_round = ['fClose', 'prev_close', 'vol_avg_2m']
df_100[cols_to_round] = df_100[cols_to_round].astype(np.float64).round(2)

sym_list = df_100['symbol'].unique().tolist()

# %% codecell
# Fib ext levels 161.8, 261.8, 423.6
# But first we need to get the fib_df level for each of the mrows
# The way to do this is to iteratere through fib_df symbols
# Then get the rows, combine them. Add hit ext levels, and a column
# For the correct one

# I'm going to want all the max row columns, and then to pick the best one
# based on that

fib_df_sub = fib_df[['symbol', 'date']].copy()
fib_df_sub['maxRow'] = 1

mrow_df = pd.merge(mrow_df, fib_df_sub, on=['symbol', 'date'], how='left')
mrow_df['maxRow'].fillna(0, inplace=True)
mrow_df.index = mrow_df['index']

# df_100.set_index('symbol', inplace=True)
# %% codecell
sym_list = df_100['symbol'].unique().tolist()
# sym_list = ['PEN']
row_list = []
fib_dict_list = []
n = 0

for sym in tqdm(sym_list):
    df_sym = df_100[df_100['symbol'] == sym].copy().reset_index(drop=True)
    max_rows = mrow_df[mrow_df['symbol'] == sym].copy()

    for index, row in max_rows.iterrows():
        if len(sym_list) < 10:
            print(row)
        max_row = max_rows[max_rows.index == index]
        rows = get_rows(df_sym, max_row, verb=False)
        fib_dict = get_fib_dict(df_sym, max_row, rows, verb=False)

        row_list.append(rows)
        fib_dict_list.append(fib_dict)

        # break

    n += 1

    if n > 10000:
        break


# %% codecell
# I need to reindex mrow_df with df_100

max_rows
# %% codecell

bpath = Path(baseDir().path, 'studies/fibonacci')

df_rows = pd.concat(row_list)
path_rows = bpath.joinpath('row_options.parquet')
write_to_parquet(df_rows, path_rows)

# %% codecell
bpath = Path(baseDir().path, 'studies/fibonacci')
df_fib_dict = pd.DataFrame.from_dict(fib_dict_list)
path_fib_dict = bpath.joinpath('fib_dict_options.parquet')

holidays_fpath = Path(baseDir().path, 'ref_data/holidays.parquet')
holidays = pd.read_parquet(holidays_fpath)
dt = getDate.query('sec_master')
current_holidays = (holidays[(holidays['date'].dt.year >= dt.year) &
                             (holidays['date'].dt.date <= dt)])
hol_list = current_holidays['date'].dt.date.tolist()
(df_fib_dict.insert(2, "date_range",
 df_fib_dict.apply(lambda row:
                   np.busday_count(row['start_date'].date(),
                                   row['end_date'].date(),
                                   holidays=hol_list),
                   axis=1)))

df_fib_dict = (pd.merge(df_fib_dict, fib_df_sub,
                        on=['symbol', 'date'], how='left'))
df_fib_dict['maxRow'].fillna(0, inplace=True)

path_fib_dict = bpath.joinpath('fib_dict_options.parquet')
write_to_parquet(df_fib_dict, path_fib_dict)

# %% codecell

df_fib_dict['maxRow']

# %% codecell
cols_to_round = ['start', 'high', 'fibPercRange', 'ext_end', 'range']
df_fib_dict[cols_to_round] = df_fib_dict[cols_to_round].astype(np.float64).round(3)

# df_fib_dict[df_fib_dict['maxRow' == 1]][['symbol', 'cond', 'date_range']]
# df_fib_dict[['symbol', 'cond', 'date_range', 'max_row']]

cols_to_merge = (['symbol', 'date', 'fChangeP', 'vol/mil', 'gap', 'cumPerc', 'vol_avg_2m', 'fHighMax'])
df_100_sub = df_100[cols_to_merge]

df_fib_comb = pd.merge(df_fib_dict, df_100_sub, on=['symbol', 'date'], how='inner')

# %% codecell
# I need to find out which of these max rows were satisfied. Let's start with
# the first max row


cols_to_merge = ['symbol', 'ext_2.618', 'ext_1.618', 'ext_4.236']
df_fib_dict_max = df_fib_dict[df_fib_dict['maxRow'] == 1][cols_to_merge]
df_100_comb = pd.merge(df_100, df_fib_dict_max, on=['symbol'], how='left')

new_cols = ['hit_1.618', 'hit_2.618', 'hit_4.236']
ext_cols = ['ext_2.618', 'ext_1.618', 'ext_4.236']

for new, ext in zip(new_cols, ext_cols):
    df_100_comb[new] = np.where(
        ((df_100_comb['symbol'] == df_100_comb['prev_symbol'])
         & (df_100_comb['fHigh'] > (df_100_comb[ext] * .98))),
         1, 0)

# %% codecell
df_hit_group = df_100_comb[new_cols + ['symbol']].groupby(by=['symbol'], as_index=False).sum()
df_hit_group = pd.merge(df_hit_group, df_100_comb[['symbol'] + ext_cols], on=['symbol'], how='left')
df_hit_group.drop_duplicates(subset=['symbol'], inplace=True)

# all_cols_to_merge =  ['symbol'] + ext_cols + new_cols
cols_to_merge = ['symbol'] + ext_cols
df_fib_hit = pd.merge(df_fib_dict, df_hit_group, on=cols_to_merge, how='left')
df_fib_hit = df_fib_hit[df_fib_hit['fibPercRange'] > .05].copy()

(356 - 348) / 348

df_fib_hit[df_fib_hit['symbol'] == 'PRFT']

# MHCP is not adjusted - needs to be adjusted
# SAVA is too early, although it's still accurate
# Accurate is row 53005 - 1/28 - 2/03
# IFS is wrong, but I wouldn't want to trade off of it anyway
all_symbols = serverAPI('all_symbols').df
all_cs = all_symbols[all_symbols['type'] == 'cs']
df_fib_cs = df_fib_hit[df_fib_hit['symbol'].isin(all_cs['symbol'].tolist())]

beta_path = Path(baseDir().path, 'studies/beta', '_2021-12-03.parquet')
df_beta = pd.read_parquet(beta_path)
df_fib_cs = pd.merge(df_fib_cs, df_beta, on=['symbol'], how='left')

beta_cond = (df_fib_cs['beta'] > 1)
fib_perc_cond = (df_fib_cs['fibPercRange'] > .20)

max_row_cond = (df_fib_cs['maxRow'] == 1)
cols_to_sub = (['symbol', 'start_date', 'ret_0.786',
                'ret_0.999', 'ext_0.999', 'ext_1.5'])
df_100_sub = pd.merge(df_100, df_fib_cs[max_row_cond][cols_to_sub], on=['symbol'], how='left')
same_sym_cond = (df_100_sub['symbol'] == df_100_sub['prev_symbol'])
dt_past_cond = (df_100_sub['date'] >= df_100_sub['start_date'])
df_100_sub = df_100_sub[same_sym_cond & dt_past_cond].copy()

larger_ret_786_cond = (df_100_sub['fClose'] > df_100_sub['ret_0.786'])
larger_ext_999_cond = (df_100_sub['fClose'] > df_100_sub['ext_0.999'])
df_100_sub['min_r786'] = np.where(larger_ret_786_cond, 1, 0)
df_100_sub['max_e999'] = np.where(larger_ext_999_cond, 1, 0)

df_100_mod = df_100_sub.set_index('symbol')
sym_list = df_100_mod.index.unique()

syms_to_check = []
for sym in tqdm(sym_list):
    sym_df = df_100_mod.loc[sym]
    if (sym_df['min_r786'].all() != 0) & (sym_df['fClose'].any() > sym_df['max_e999'].iloc[0]):
        syms_to_check.append(sym)

len(syms_to_check)
syms_to_check

df_100_larger_999 = df_100_sub[larger_maxe999_cond].copy()
df_100_larger_786 = df_100_sub[larger_minr786_cond].copy()

df_100_larger_786

df_100_larger_999

cols_to_group = ['symbol', 'fClose', 'min_r786', 'max_e999']
df_100_sub[cols_to_group].groupby(by='symbol')

df_100_sub


df_fib_cs.columns

df_fib_cs[beta_cond & fib_perc_cond].dropna(subset=['hit_4.236']).sort_values(by=['date_range'], ascending=False).head(25)

df_fib_hit.dropna(subset=['hit_4.236']).sort_values(by=['hit_4.236'], ascending=False).head(25)



# %% codecell

# I'd like to calculate fibonacci sequence values using a few
# of the ones that I know work for sure




# %% codecell


# %% codecell
"""
dt = getDate.query('iex_eod')
path = Path(baseDir().path, 'studies/beta', f"_{dt}.parquet")
write_to_parquet(beta_df, path)

beta_df_copy = beta_df.copy()

fib_df = pd.merge(fib_df, beta_df_copy, on=['symbol'], how='left')

"""

# %% codecell
