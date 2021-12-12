"""Analyzing max rows for conditions."""
# %% codecell
from pathlib import Path
import pandas as pd
import numpy as np
import importlib
import sys
from tqdm import tqdm

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_parquet, getDate

from workbooks.fib_funcs import get_max_rows, get_rows, get_fib_dict, get_diff_dict, make_confirm_df, read_clean_combined_all, write_fibs_to_parquet
importlib.reload(sys.modules['workbooks.fib_funcs'])
from workbooks.fib_funcs import get_max_rows, get_rows, get_fib_dict, get_diff_dict, make_confirm_df, read_clean_combined_all, write_fibs_to_parquet


# %% codecell
pd.set_option('display.max_columns', 30)

# %% codecell
path = Path(baseDir().path, 'studies/fibonacci', 'mrow_options.parquet')
mrow_df = pd.read_parquet(path)
# %% codecell
mrow_df = mrow_df[mrow_df['cond'] != 'None'].copy()
mrow_df.drop_duplicates(inplace=True)

# %% codecell
df_all = read_clean_combined_all()
df_all = df_all[df_all['date'] >= '2021']

df_all.head()
# %% codecell

df_all['prev_close'] = df_all['fClose'].shift(periods=1, axis=0)
df_all['prev_symbol'] = df_all['symbol'].shift(periods=1, axis=0)
not_the_same = df_all[df_all['symbol'] != df_all['prev_symbol']]
df_all.loc[not_the_same.index, 'prev_close'] = np.NaN

# 52 week high. For loop is computationally expensive
# 3 minutes to run through all symbols ytd. Doesn't make sense
df_all

df_all['fHighMax'] = np.NaN
max = 0
symbol = df_all['symbol'].iloc[0]
n = 0

df_all['fHighMax'] = np.where(
                        (df_all['symbol'] == df_all['prev_symbol'])
                        & (df_all[''])
                    )

# Case where prev_close = np.NaN, fHighMax should also equal fHighMax

max = 0
max_test = []

for index, row in tqdm(df_all[['symbol', 'fHigh', 'prev_symbol']].iterrows()):
    if not row['prev_symbol']:
        max = 0
    if row['fHigh'] > max:
        max = row['fHigh']
        max_test.append(row['fHigh'])
    else:
        max_test.append(np.NaN)


df_all['fHighMax'] = max_test

# %% codecell


# %% codecell

path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals.parquet')
fib_df = pd.read_parquet(path)

fib_df[fib_df['symbol'] == 'ANIP']
# %% codecell

# Volume doesn't work here - range does (fChangeP)
# There's no clear sequence
# 3-31 does work though

# Sort through fHighs from ytd
# Alright so we can automatically exclude 2 where the ytd high isn't there
[~df_all['fHighMax'].isna()]

test_rows  = (df_all.loc[df_all[df_all['symbol'] == 'ANIP']['fChangeP'].nlargest(10).index]
                    .loc[~df_all['fHighMax'].isna()]
                    .sort_values(by=['date'], ascending=True)
                    .reset_index(drop=True)
              )

row_dict = {}

symbol = 'ANIP'
df_sym = (df_all[df_all['symbol'] == symbol]
          .sort_values(by=['date'], ascending=True)
          .reset_index(drop=True)
          .copy())

test_rows  = (df_sym.loc[df_sym['fChangeP'].nlargest(10).index]
                    .loc[~df_sym['fHighMax'].isna()]
                    .sort_values(by=['date'], ascending=True)
              )
df_sym['fChangeP'].nlargest(10).tolist()
row = max_row
row
row['fHighMax'].isna().values[0]
row['fChangeP'].isin(df_sym['fChangeP'].nlargest(10).tolist()).values[0]
# %% codecell
if ((row['fChangeP'].isin(df_sym['fChangeP'].nlargest(10).tolist()).values[0])
    & (not row['fHighMax'].isna().values[0])):
    print('True')

# %% codecell
test_rows
max_row = test_rows.iloc[0]

if (max_row['fHighMax'] != np.NaN):
    print('Not NAN')
max_row, max_rows = get_max_rows(df_sym, verb=False)


max_row = test_rows.iloc[0].to_frame().T

max_rows
max_row
row_dict = {}

for index, row in max_rows.iterrows():
    max_row = max_rows[max_rows.index == index]
    row_dict[index] = get_rows(df_sym, max_row, verb=False)

row_list = []
for value in row_dict.values():
    row_list.append(value)

row_df = pd.concat(row_list)
max_rows = max_rows.join(row_df['date_range'])
max_rows.sort_values(by=['date_range'], ascending=False)
row_df
# %% codecell

# %% codecell
def make_confirm_df(rows, index, diff_dict, fib_dict, df_confirm_all):
    """Make confirm df from acceptable values."""
    cutoff = 0.025
    confirm_list = []
    cols_to_check = ['fOpen', 'fHigh', 'fLow', 'fClose']
    # We could even stop here. That might be enough honestly
    col_list = cols_to_check * rows.shape[0]
    symbol = rows['symbol'].iloc[0]
    dt_list = [[str(row['date'].date())] * 4 for index, row in rows.iterrows()]
    dt_list = [dt for sublist in dt_list for dt in sublist]

    for key in diff_dict.keys():
        for val, dt, col in zip(diff_dict[key], dt_list, col_list):
            if abs(val) < cutoff:
                confirm_list.append((symbol, key, dt, col, round(val, 2)))
                # print(key, dt.date(), col, round(val, 2))

    if confirm_list:
        confirm_cols = ['symbol', 'fib', 'date', 'col', 'perc_diff']
        confirm_df = pd.DataFrame(confirm_list, columns=confirm_cols)
        confirm_df['date'] = pd.to_datetime(confirm_df['date'])
        confirm_df['index'] = index
        confirm_df['length'] = len(row_dict[index])
        confirm_df['fib_val'] = confirm_df['fib'].map(fib_dict)
        # .round(2)
        # Append rows to df_confirm_all
        df_confirm_all = df_confirm_all.append(confirm_df)
    return df_confirm_all


def get_top_row_pred(confirm_all_dict, row_dict, verb=False):
    """Return top row prediction based on conditions."""
    confirm_list = [confirm_all_dict[key] for key in confirm_all_dict.keys()]
    confirm_all = pd.concat(confirm_list)

    con_summary = (confirm_all[['index', 'length', 'col']]
                   .groupby(by=['index', 'length'], as_index=False)
                   .count()
                   .rename(columns={'col': 'count'}))

    fHigh_list, fLow_list = [], []

    for index, row in con_summary.iterrows():
        fHigh_list.append(row_dict[row['index']]['fHigh'].max())
        fLow_list.append(row_dict[row['index']]['fLow'].min())

    con_summary['fLow'] = fLow_list
    con_summary['fHigh'] = fHigh_list

    con_summary['fPercRange'] = (((con_summary['fHigh'] - con_summary['fLow'])
                                   / con_summary['fLow']).round(3))

    top_fPercRange = con_summary.loc[con_summary['fPercRange'] == con_summary['fPercRange'].max()]
    if top_fPercRange.shape[0] > 1:
        top_fPercRange = top_fPercRange.loc[top_fPercRange['index'] == top_fPercRange['index'].max()]

    if verb:
        print(top_fPercRange)
    return top_fPercRange

# %% codecell

df_all[df_all['prev_symbol'].isna()]
df_all[(df_all['symbol'] == 'SWK') & (df_all['date'].dt.month == 1)]

df_all[(df_all['symbol'] == 'SWK') & (df_all['date'].dt.month == 3)]

# %% codecell

# There are several ways to solve this problem.
# One is getting local maximums, then working my way back from there

# Another is coming up with each max row and running an optimization problem
# Using the fibonacci difference to minimize differences

# I'd have to change the structure of my functions, which is fine. I don't mind that part
# The max range will be interesting

# Max vol works for MARA but not necessarily for the others
# I don't necessarily have to optimize based on one column condition
# Certain columns can be fine for certain stocks
# %% codecell
from workbooks.fib_funcs import get_max_rows, get_rows, get_fib_dict, get_diff_dict, make_confirm_df, read_clean_combined_all, write_fibs_to_parquet
importlib.reload(sys.modules['workbooks.fib_funcs'])
from workbooks.fib_funcs import get_max_rows, get_rows, get_fib_dict, get_diff_dict, make_confirm_df, read_clean_combined_all, write_fibs_to_parquet

# %% codecell

df_all = read_clean_combined_all()

mrow_empty_list = []
mrows_empty_list = []
# %% codecell

confirm_cols = ['symbol', 'fib', 'date', 'col', 'perc_diff']
df_confirm_all = pd.DataFrame(columns=confirm_cols)
fib_dict_list = []
mrows_list = []
cutoff = .025
# symbol_list = df_all['symbol'].unique().tolist()
path = Path(baseDir().path, 'studies/fibonacci', 'top_50_beta.parquet')
top_50_beta = pd.read_parquet(path)
symbol_list = top_50_beta['symbol'].tolist()

symbol_list = ['SWK']
# Still need to account for gaps in either direction
n = 0
len_cutoff = 100

for symbol in tqdm(symbol_list):
    df_sym = (df_all[df_all['symbol'] == symbol]
              .sort_values(by=['date'], ascending=True)
              .reset_index(drop=True)
              .copy())
    # Onto the next iteration if less than 50 days of data
    if df_sym.shape[0] < 50:
        continue
    max_row, max_rows = get_max_rows(df_sym, verb=True)
    # I'd like to see all max_row possibilities
    if not max_rows.empty:
        mrows_list.append(max_rows)

    print(f"Max row is {max_row}")

    row_dict = {}
    fib_dict_all = {}
    diff_dict_all = {}
    confirm_all_dict = {}
    confirm_cols = ['symbol', 'fib', 'date', 'col', 'perc_diff']
    df_confirm_sym = pd.DataFrame(columns=confirm_cols)

    for index, row in max_rows.iterrows():
        max_row = max_rows[max_rows.index == index]
        row_dict[index] = get_rows(df_sym, max_row, verb=False)
        fib_dict_all[index] = get_fib_dict(df_sym, max_row, row_dict[index], verb=False)
        diff_dict_all[index] = get_diff_dict(fib_dict_all[index], row_dict[index], cutoff)
        confirm_all_dict[index] = make_confirm_df(row_dict[index], index, diff_dict_all[index], fib_dict_all[index], df_confirm_sym)


    not_empty = False
    for item in confirm_all_dict.items():
        try:
            if not item.empty:
                not_empty = True
                break
        except AttributeError:
            if len(item[1]) > 1:
                not_empty = True
                break

    if not not_empty:
        print(f"Confirm dict for {symbol} is empty")
    else:
        top_row = get_top_row_pred(confirm_all_dict, row_dict, verb=False)
        top_index = top_row['index'].iloc[0]
        fib_dict_list.append(fib_dict_all[top_index])
        df_confirm_all = df_confirm_all.append(confirm_all_dict[top_index])

    n += 1
    if n > (len_cutoff - 1):
        break

if (len(symbol_list) > 25) | (len(fib_dict_list) > 25):
    write_fibs_to_parquet(df_confirm_all, fib_dict_list)
    df_mrows = pd.concat(mrows_list).reset_index()
    path = Path(baseDir().path, 'studies/fibonacci', 'mrow_options.parquet')
    df_mrows['cond'] = df_mrows['cond'].astype('str')
    print('Writing dataframe to parquet')
    write_to_parquet(df_mrows, path)



# %% codecell

# Can also ignore dojis - which I can use TA-Lib to identify

confirm_all_test = [val for key, val in confirm_all_dict.items()]
confirm_df_test = pd.concat(confirm_all_test)
confirm_df_test['date'].value_counts()

row_test = [row for row in row_dict.values()]
row_df = pd.concat(row_test)

row_df
max_rows
# %% codecell

# %% codecell

# %% codecell



max_rows[max_rows['date'] == max_rows[max_rows['fChangeP'].isin(max_rows['fChangeP'].nlargest(3))]['date'].min()]

 row['fChangeP'].isin(max_rows)
(row == max_rows['date'].min())

# Clear and obvious - top 3 volume, largest percentage change, and earliest date

((row['fVolume'].isin(max_rows['fVolume'].nlargest(3)))
& (row['fChangeP'].isin(max_rows['fChangeP'].nlargest(3)))
& (row['date'].isin(max_rows['date'].nsmallest(2))))

max_rows['date'].nsmallest(2)

# %% codecell
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

max_rows['date_doy'] = max_rows['date'].dt.day_of_year
# X = [[x, y] for x,y in zip(max_rows.index, max_rows['date_doy'].values.tolist())]
X = [[x, y] for x,y in zip(max_rows['date_doy'].values.tolist(), max_rows['fRange'].values.tolist())]
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



max_rows




# %% codecell
