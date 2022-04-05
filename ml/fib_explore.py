"""Fibonacci sequence data workbook."""
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

# %% codecell
from workbooks.fib_funcs import read_clean_combined_all
pd.set_option('display.max_columns', 30)


from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

from sklearn import model_selection
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, accuracy_score
from sklearn.metrics import mean_squared_error
from math import sqrt

# %% codecell

cols_to_exclude = ['perc_2weeks']
df_pre = df_hist_sub.drop(columns=cols_to_exclude, errors='ignore').copy()

df_pre.drop(columns=['prev_symbol', 'gRange'], inplace=True, errors='ignore')
dt_columns = ['start_date', 'end_date', 'date', 'filing']
df_non_dt_columns = list(set(df_pre.columns.tolist()) - set(dt_columns))

for dt in dt_columns:
    df_pre[f"{dt}_month"] = pd.DatetimeIndex(df_pre[dt]).month
    df_pre[f"{dt}_day"] = pd.DatetimeIndex(df_pre[dt]).day
    df_pre[f"{dt}_dayofyear"] = pd.DatetimeIndex(df_pre[dt]).dayofyear
    df_pre[f"{dt}_weekday"] = pd.DatetimeIndex(df_pre[dt]).weekday
    df_pre[f"{dt}_quarter"] = pd.DatetimeIndex(df_pre[dt]).quarter
    df_pre[f"{dt}_is_month_start"] = pd.DatetimeIndex(df_pre[dt]).is_month_start
    df_pre[f"{dt}_is_month_end"] = pd.DatetimeIndex(df_pre[dt]).is_month_end

df_pre.drop(columns=dt_columns, inplace=True)
all_dt_columns = list(set(df_pre.columns.tolist()) - set(df_non_dt_columns))
df_pre[all_dt_columns] = df_pre[all_dt_columns].astype('category')

df_pre['cond'] = df_pre['cond'].astype('object')
df_pre['cond'].fillna(-1, axis=0, inplace=True)
df_pre['prev_close'].fillna(method='ffill', inplace=True)
df_pre['fHighMax'].fillna(method='ffill', inplace=True)
df_pre['cumPerc'].fillna(method='ffill', inplace=True)
if 'perc_1month' in df_pre.columns.tolist():
    df_pre['perc_1month'].fillna(method='ffill', inplace=True)

def cat_encode(df, col):
    """Encode categorical to num list."""
    col_vc = df[col].value_counts()
    con_n_list = list(range(len(col_vc.index)))

    col_dict = {key: n for key, n in zip(col_vc.index, con_n_list)}
    df[col] = df[col].map(col_dict)

    return df, col_dict

df_pre, cond_dict = cat_encode(df_pre, 'cond')
df_pre, sym_dict = cat_encode(df_pre, 'symbol')

# %% codecell
other_cols_to_cat = ['symbol', 'cond', 'days_until']
df_pre[other_cols_to_cat] = df_pre[other_cols_to_cat].astype('category')

na_cols = df_pre.columns[df_pre.isna().any()].tolist()
df_pre.loc[0, na_cols] = 0

na_cols = df_pre.columns[df_pre.isna().any()].tolist()
if na_cols:
    print(na_cols)

path = Path(baseDir().path, 'ml_data/fib_analysis', 'fib_prepped_data.parquet')
write_to_parquet(df_pre, path)

len_of_syms = len(df_pre['symbol'].value_counts().index)
df_pre['hit_2.618'].value_counts()[1] / len_of_syms



df_pre.head()

df_pre.info()

# %% codecell

# %% codecell

train, test = train_test_split(df_pre, test_size=0.2, random_state=25)

target_column_train = ['hit_2.618']
predictors_train = list(set(list(train.columns))-set(target_column_train))

X_train = train[predictors_train].values
y_train = train[target_column_train].values

target_column_test = ['hit_2.618']
predictors_test = list(set(list(test.columns))-set(target_column_test))

X_test = test[predictors_test].values
y_test = test[target_column_test].values

# %% codecell



# %% codecell

# dtree = DecisionTreeRegressor(max_depth=8, min_samples_leaf=0.13, random_state=3)
dtree = DecisionTreeClassifier()
dtree.fit(X_train, y_train)

# Code lines 1 to 3
pred_train_tree = dtree.predict(X_train)
print("X_train rmse score: ")
print(np.sqrt(mean_squared_error(y_train,pred_train_tree)))
print("X_train R2 score: ")
print(r2_score(y_train, pred_train_tree))
print()

# Code lines 4 to 6
print("X_test rmse score: ")
pred_test_tree = dtree.predict(X_test)
print(np.sqrt(mean_squared_error(y_test,pred_test_tree)))
print("X_test R2 score: ")
print(r2_score(y_test, pred_test_tree))
# %% codecell
print("Accuracy:", accuracy_score(y_train, pred_train_tree))
print("Accuracy:", accuracy_score(y_test, pred_test_tree))

import graphviz
from sklearn import tree
dot_data = tree.export_graphviz(dtree, out_file=None)

tree.plot_tree(dtree)

# %% codecell
