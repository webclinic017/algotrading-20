"""Looking for stocks with 2-3x avg volume, no movement, low market cap."""
# %% codecell
from tqdm import tqdm

import pandas as pd
import numpy as np

from api import serverAPI
from workbooks.fib_funcs import read_clean_combined_all
from multiuse.symbol_ref_funcs import get_symbol_stats

# %% codecell

# Scan ideas:
# If an after hour bar is within the 5 largest bars of the day
# Check on that urgently

df_all = read_clean_combined_all()


# %% codecell

# I definitely need the highest point from the last year
# While I'm at it I can get the hight price, highest close
df_max_mod = df_all.groupby(by=['symbol']).max()[['fHigh']]

df_max = pd.merge(df_all, df_max_mod, on=['symbol', 'fHigh'])

# The high price shouldn't be more than 2x the current price
df_max

# %% codecell

df_all['month'] = df_all['date'].dt.month
df_vol_avg = (df_all[['symbol', 'month', 'fVolume']]
              .groupby(by=['symbol', 'month'], as_index=False)
              .mean()
              .rename(columns={'fVolume': 'fVolmAvg'}))
df_vol_avg['fVolmAvg'] = df_vol_avg['fVolmAvg'].div(1000000)

df_all = pd.merge(df_all, df_vol_avg, on=['symbol', 'month'])

df_stats_all = get_symbol_stats()
df_stats_all.columns
df_stats = df_stats_all[['symbol', 'sharesOutstanding', 'marketcap', 'beta', 'week52change']]
df_all = pd.merge(df_all, df_stats, how='left', on=['symbol'])
df_all['perc/shares'] = (df_all['fVolume'].div(df_all['sharesOutstanding'])).round(2)

top_percs = df_all[['symbol', 'week52change']].drop_duplicates().sort_values(by='week52change', ascending=False)
top_percs = top_percs[top_percs['week52change'] > 50]
top_percs
# Ideally I'd like to see vol 2-3x normal, and little to no change in price
# And I'd like to see this for three days
# %% codecell

sym_list = df_all['symbol'].unique().tolist()

n = 0
row_list = []
# Can try to find individual rows that match condition, then
# Workoutwards from there
# cond_1 = (df_mod['perc/shares'] > .1)
# cond_2 = ((df_mod['fChangeP'] > -.05) & (df_mod['fChangeP'] < .05))
# cond_3 = (df_mod['fClose'] < 10)

for sym in tqdm(sym_list):
    df_mod = df_all[df_all['symbol'] == sym].copy()

    if df_mod.shape[0] < 150:
        continue

    cond_1 = (df_mod['perc/shares'] > .3)
    cond_2 = ((df_mod['fChangeP'] > -.05) & (df_mod['fChangeP'] < .05))
    cond_3 = (df_mod['fClose'] < 10)
    rows = df_mod[cond_1 & cond_2 & cond_3]

    if not rows.empty:
        row_list.append(rows)

    n += 1
    if n > 1000:
        break



# %% codecell

df_rows = pd.concat(row_list)
df_rows_syms = df_rows['symbol'].unique().tolist()
len(df_rows_syms)
# AADI - ACY - ADXS - AEMD - AHPI - AMPG
# ANY - ATOS - AUUD - AUVI - BLIN


df_rows_syms



# %% codecell
