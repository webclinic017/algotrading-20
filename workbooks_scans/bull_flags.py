"""Bull flag pattern identification."""
# %% codecell

from pathlib import Path
import importlib
import sys

import pandas as pd
import numpy as np
from tqdm import tqdm

from workbooks.fib_funcs import read_clean_combined_all
from api import serverAPI

importlib.reload(sys.modules['workbooks.fib_funcs'])
importlib.reload(sys.modules['api'])

from workbooks.fib_funcs import read_clean_combined_all
# %% codecell
pd.set_option('display.max_columns', 50)
# %% codecell

df_all = read_clean_combined_all(local=False)

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



# %% codecell

cond1 = (df_all['fCP5'].between(0.10, 0.25))
cond2 = (df_all['fCP5'].shift(5, axis=0).between(-.05, 0.05))
cond3 = (df_all['fCP5'].shift(-5, axis=0).between(-.025, 0.025))

run_up_rows = df_all[cond1 & cond2 & cond3]
run_up_rows



# %% codecell

# I'd like to see big drops (greater than 10%)
# How long until the gaps get filled?

cond1 = (df_all['gap'] < -0.10)
cond2 = (df_all['fChangeP'].shift(axis=0) < .05)

gap_down = df_all[cond1]

# %% codecell
df_all['fChangeP'].shift(axis=0)

# df_all[df_all['gap'] == 1]
gap_down



# %% codecell
