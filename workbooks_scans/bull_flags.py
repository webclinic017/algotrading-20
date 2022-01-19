"""Bull flag pattern identification."""
# %% codecell

from pathlib import Path
import importlib
import sys

import pandas as pd
import numpy as np
from tqdm import tqdm

from workbooks.fib_funcs import read_clean_combined_all
from charting.plt_standard import plot_cols
from multiuse.pd_funcs import mask, chained_isin
from multiuse.help_class import baseDir, getDate, write_to_parquet
from multiuse.path_helpers import get_most_recent_fpath
from api import serverAPI

importlib.reload(sys.modules['workbooks.fib_funcs'])
importlib.reload(sys.modules['charting.plt_standard'])
importlib.reload(sys.modules['api'])
importlib.reload(sys.modules['multiuse.help_class'])

from workbooks.fib_funcs import read_clean_combined_all, add_gap_col
# %% codecell
pd.DataFrame.mask = mask
pd.DataFrame.chained_isin = chained_isin

pd.set_option('display.max_columns', 50)
# %% codecell

# %% codecell

# %% codecell

# df_all = read_clean_combined_all(local=True)

# %% codecell

# %% codecell

fpath = Path(baseDir().path, 'ml_data/fib_analysis/df_all_temp.parquet')
df_all = pd.read_parquet(fpath)
df_all = add_gap_col(df_all)

# %% codecell

df_all_cols = df_all.columns
cols_to_round = (['fOpen', 'fLow', 'fClose', 'fHighMax', 'prev_close',
                  'rsi', 'vol_avg_2m', 'fCP5', 'sma_50', 'sma_200'])
df_all[cols_to_round] = df_all[cols_to_round].astype(np.float64).round(2)
df_all.reset_index(drop=True, inplace=True)

# 1. Period of little movement for 2+ weeks.
# 2. Period of major up movement
# 3. Fib retracement fof 38-50%
# 4. Slight decline

# %% codecell

df_all

# %% codecell

cond1 = (df_all['fCP5'].between(0.10, 0.25))
cond2 = (df_all['fCP5'].shift(5, axis=0).between(-.05, 0.05))
cond3 = (df_all['fCP5'].shift(-5, axis=0).between(-.025, 0.025))

run_up_rows = df_all[cond1 & cond2 & cond3]
run_up_rows



# %% codecell

# I'd like to see big drops (greater than 10%)
# How long until the gaps get filled?

cond1 = (df_all['gap'] < -0.20)
cond2 = ((df_all.shift(periods=5, axis=0)['fLow'] > df_all['fHigh']) & (df_all.shift(periods=5, axis=0)['symbol'] == df_all['symbol']))

gap_down = df_all[cond1 & cond2]

gap_down['gap'].describe()

# Find all
syms_to_exp = gap_down['symbol'].unique()
df_all_syms = df_all.set_index('symbol').copy()
gap_du_list = []
gap_days_dict = {sym: {} for sym in gap_down['symbol'].unique()}
keys_to_create = ['cov', 'gapDate', 'firstDate']

for index, row in tqdm(gap_down.iterrows()):
    df_test = df_all_syms.loc[row['symbol']]

    df_sym = df_all_syms.loc[row['symbol']].copy()
    df_future = df_sym[(df_sym['date'] > row['date']) & (df_sym['fLow'] > row['fHigh'])]
    base_dict = gap_days_dict[row['symbol']]

    if df_future.empty:
        for col in keys_to_create:
            base_dict[col] = np.NaN
        continue

    base_dict['cov'] = df_future.shape[0] / df_sym.shape[0]
    base_dict['gapDate'] = row['date']
    base_dict['firstDate'] = df_future.head(1)['date'].iloc[0]


    """
    if_cond1 = (row['fHigh'] < df_future.head(15)['fLow'].min())
    if_cond2 = (row['symbol'] == df_future['prev_symbol'].shift(periods=-16, axis=0)).tail(1).iloc[0]

    if if_cond1 & if_cond2:
        gap_du_list.append(row[['symbol']])
    """
# %% codecell
col_list_dict = {}
for col in keys_to_create:
    col_list_dict[col] = [gap_days_dict[sym][col] for sym in gap_days_dict.keys()]

df_dict_days = pd.DataFrame.from_dict(gap_days_dict.keys())

for col in keys_to_create:
    df_dict_days[col] = col_list_dict[col]

df_dict_days.rename(columns={0: 'symbol'}, inplace=True)
df_dict_days.dropna(inplace=True)
df_dict_days['cov'] = df_dict_days['cov'].round(3)

df_dict_days.sort_values(by=['cov'], ascending=False)



df_dict_days.insert(2, 'dayDiff', getDate.get_bus_day_diff(df_dict_days, 'gapDate', 'firstDate'))

# df_all[df_all['gap'] == 1]

# %% codecell

df_conds = pd.merge(df_dict_days, df_all, left_on=['symbol', 'gapDate'], right_on=['symbol', 'date'])
df_conds.mask('fClose', 5, equals=False, greater=True)

df_conds

df_dict_days[['cov', 'dayDiff']].corr()
df_dict_days


# %% codecell


# %% codecell

plot_cols(df_all.mask('symbol', 'PBR'), vol=True, figsize=(32, 10))





gap_down



# %% codecell
