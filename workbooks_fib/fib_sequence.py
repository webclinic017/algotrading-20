"""
Nasdaq data pull.

Q - NASDAQ Global Select Market (NGS)
R - NASDAQ Capital Market
Short data comes out every day past 4:30 pm
"""

# %% codecell
############################################
import sys
from pathlib import Path
import importlib
from tqdm import tqdm

import datetime

import pandas as pd
import numpy as np


try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripst.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions
    from multiuse.path_helpers import get_most_recent_fpath
    from api import serverAPI


# %% codecell
############################################
# %% codecell


# %% codecell
# %% codecell

# %% codecell
# %% codecell

# %% codecell
# %% codecell
# %% codecell


# %% codecell
from workbooks_fib.fib_funcs import get_max_rows, get_rows, get_fib_dict, get_diff_dict, make_confirm_df, read_clean_combined_all, write_fibs_to_parquet
importlib.reload(sys.modules['workbooks_fib.fib_funcs'])
from workbooks_fib.fib_funcs import get_max_rows, get_rows, get_fib_dict, get_diff_dict, make_confirm_df, read_clean_combined_all, write_fibs_to_parquet

# %% codecell
dt = datetime.date(2021, 1, 1)

df_all = read_clean_combined_all(local=False, dt=dt)

mrow_empty_list = []
mrows_empty_list = []

# %% codecell
# dump_path = Path(baseDir().path, 'dump', 'df_all_cleaned_max.parquet')
# write_to_parquet(df_all, dump_path)

# %% codecell
confirm_cols = ['symbol', 'fib', 'date', 'col', 'perc_diff']
df_confirm_all = pd.DataFrame(columns=confirm_cols)
fib_dict_list = []
mrows_list = []
cutoff = .025
symbol_list = df_all['symbol'].unique().tolist()

# symbol_list = ['SWK']
# Still need to account for gaps in either direction
n = 0

for symbol in tqdm(symbol_list):
    df_sym = (df_all[df_all['symbol'] == symbol]
              .sort_values(by=['date'], ascending=True)
              .reset_index(drop=True)
              .copy())
    # Onto the next iteration if less than 50 days of data
    if df_sym.shape[0] < 50:
        continue

    max_row, max_rows = get_max_rows(df_sym, verb=False)
    # If high or range is 0
    if (max_row['fHigh'].iloc[0] == 0) or (max_row['fRange'].iloc[0] == 0):
        continue
    # I'd like to see all max_row possibilities
    if not max_rows.empty:
        mrows_list.append(max_rows)

    rows = get_rows(df_sym, max_row, verb=False)
    # If rows is empty
    if rows is None:
        continue
    elif rows.empty:
        print(f"rows for symbol: {df_sym['symbol'].iloc[0]} is empty!")
        mrow_empty_list.append(max_row)
        mrows_empty_list.append(max_rows)
        continue

    fib_dict = get_fib_dict(df_sym, max_row, rows, verb=False)
    # Add fib dict to list
    fib_dict_list.append(fib_dict)
    diff_dict = get_diff_dict(fib_dict, rows, cutoff)
    df_confirm_all = make_confirm_df(rows, cutoff, diff_dict, fib_dict, df_confirm_all)

    n += 1
    if n > 10000:
        break

if len(symbol_list) > 50:
    write_fibs_to_parquet(df_confirm_all, fib_dict_list)
    df_mrows = pd.concat(mrows_list).reset_index()
    path = Path(baseDir().path, 'studies/fibonacci', 'mrow_options.parquet')
    df_mrows['cond'] = df_mrows['cond'].astype('str')

    write_to_parquet(df_mrows, path)


# %% codecell


# max_row = max_rows[max_rows['date'] == '2021-03-02']
# max_row

rows = get_rows(df_sym, max_row, verb=False)

fib_dict = get_fib_dict(df_sym, max_row, rows, verb=False)

# Need to adjust for dividends - as of right now this isn't entirely accurate
fib_dict

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# X = [[x, y] for x,y in zip(max_rows.index, max_rows['date_doy'].values.tolist())]
X = [[x, y] for x,y in zip(max_rows['date_doy'].values.tolist(), max_rows['fHigh'].values.tolist())]
cluster_dict, sil = {}, {}
for k in range(1, 10):
    try:
        km = KMeans(n_clusters=k).fit(X)
        cluster_dict[k] = km.inertia_
        sil[k] = silhouette_score(X, km.labels_, metric = 'euclidean')
    except ValueError:
        pass

cluster_dict
sil

# What if I make the fibonacci extensions after making the retracements
# Then test different values and pick the one with the greatest likelihood of success

# %% codecell
try:
    df_em_mrow = pd.concat(mrow_empty_list)
    df_em_mrows = pd.concat(mrows_empty_list)
except ValueError:
    pass

df_mrows = pd.concat(mrows_list).reset_index()
path = Path(baseDir().path, 'studies/fibonacci', 'mrow_options.parquet')
df_mrows['cond'] = df_mrows['cond'].astype('str')

write_to_parquet(df_mrows, path)

# %% codecell
df_em_mrow[df_em_mrow['symbol'] == 'PLUG']

df_em_mrows[df_em_mrows['symbol'] != 'PLUG']

# There's an interesting part here where the first major move
# predicts the rest. So I could see a situation where
# the first date can also be used for extensions

# What I can do is not change a thing with my overall code (which is fine)
# But run a separate for loop using most of the same code
# Then find the earliest occurence where this happens

# Fib ratios below $5 are not nearly as useful.
# Needs to be a significant movement

# Here's another idea - avg volume increases significantly over
# a few days and the price doesn't change at all.
# More importantly, the volume is sustained for at least 3 days with
# Little price movement


# %% codecell


# %% codecell
symbol_list = df_mrow['symbol'].tolist()
# I'd rather not deal with situations where there isn't a next or previous row
# %% codecell

# %% codecell

# %% codecell



# %% codecell

# %% codecell


# %% codecell

# fib_df.info()

fib_df[fib_df['symbol'] == 'MARA']

rows
max_row
df_sym


df_all[df_all.index == 31493]

rows
max_row
fib_dict

row_vals = rows[cols_to_check].values
row_vals
for key in fib_dict.keys():
    print(key, type(fib_dict[key]))
    print()
rows

max_row['symbol'].iloc[0]
max_row
df_all[(df_all['symbol'] == max_row['symbol'].iloc[0]) & (df_all['date'].dt.month == max_row['date'].iloc[0].date().month)]
df_sym
# %% codecell
# Rules - there should only be one fibonacci movement
# Limit to major runs - one to two candles
# Start with the highest volume for the time period (coincidentally the highest % change also)

# %% codecell
# %% codecell

# Can go forward one, back one, to see the accuracy of our fibonacci range prediction
# So with the one, the first thing I'd like to see is if the retracement lines make sense.
# Should be apparent within a day or two

# Would be nice to see a rolling avg of volume to compare against
# But I know this also works on big % change (square earnings)


# If rows is one row, then we need 4 dates
# If rows is two rows


# dt_list = rows['date'].tolist() * df_diff.shape[0]
# tuple_list = [(dt, col) for dt, col in zip(dt_list, col_list)]
# df_diff = pd.DataFrame.from_records(diff_dict)
# df_diff.index = pd.MultiIndex.from_tuples(tuple_list, names=['date', 'col'])

# confirm_df['col_val'] = confirm_df.apply(lambda row: rows[rows['date'] == row['date']].loc[row['col']], axis=1)

# I'll need to add the row(s) to a master df
df_confirm_all = df_confirm_all.append(confirm_df)


# %% codecell
# Do I really care which of the columns matches up?
# Not really. I don't quite see how that's important here.
# It could be any column. We could set the shape cutoff at 2. So at least 2
# Of the column values

df_abs_diff = pd.DataFrame(df_diff.unstack()).rename(columns={0: 'abs_diff'}).abs()
df_abs_diff[df_abs_diff['abs_diff'] < cutoff]
df_diff = df_diff.reset_index().rename(columns={'index': 'column'})
df_diff.iloc[0]

diff_dict


# %% codecell


# So you take the original fibonacci range and then use
# That to find the midpoint for fibonacci extensions. 50% of the range
# What's interesting is that any of the fibonacci sequence can be used as the midpoint

for fib in fib_percs[1:-1]:
    print(range * fib)
range * .50

for fib in fib_percs[1:-1]:
    print(range * fib)

# Bull flag pattern recognition
# Look for big movement up - greater than 25% over at a minimum 5 trading days
# Then draw a line from the high post initial run to the high from below
# Get the slope
# Do the same on the intiial run up
# Then do the same on at least two lows


# %% codecell


# %% codecell


# %% codecell

# %% codecell


# %% codecell


# %% codecell

# %% codecell


# %% codecell


# %% codecell
