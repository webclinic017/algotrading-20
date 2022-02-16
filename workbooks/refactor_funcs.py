"""Refactoring for loop functions for df_all."""
# %% codecell
from pathlib import Path
import importlib
import sys

import pandas as pd
import numpy as np
from tqdm import tqdm
import talib
import yfinance as yf

from multiuse.help_class import baseDir, getDate, write_to_parquet, dataTypes
from multiuse.path_helpers import get_most_recent_fpath
from multiuse.pd_funcs import mask, chained_isin

from studies.add_study_cols import add_gap_col, calc_rsi, make_moving_averages, add_fChangeP_col, add_fHighMax_col
importlib.reload(sys.modules['studies.add_study_cols'])
from studies.add_study_cols import add_gap_col, calc_rsi, make_moving_averages, add_fChangeP_col, add_fHighMax_col

from api import serverAPI
# %% codecell
pd.DataFrame.mask = mask
pd.DataFrame.chained_isin = chained_isin

dump_path = Path(baseDir().path, 'dump', 'df_all_cleaned_max.parquet')

df_all = pd.read_parquet(dump_path).copy()
# %% codecell

# %% codecell

# %% codecell

# %% codecell
path = Path(baseDir().path, 'dump', 'refact_fib_data.parquet')
# write_to_parquet(df_all, path)
# %% codecell

df_all = pd.read_parquet(path).copy()

# %% codecell

# %% codecell

# %% codecell


# %% codecell

# I'd really like to get the yfinance historical data on my server,
# rather than just keeping local copies.


# %% codecell

# %% codecell

# There has to be some easy way to record all NaN days

# Need to get the year unique values

# For loop should work. This would be much easier
# if I did it in the bulk download function
# I'm also going to need to combined based on year
# %% codecell




# %% codecell


# %% codecell



# %% codecell
