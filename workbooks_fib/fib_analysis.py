"""Fibonacci pattern analysis."""
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
import talib

try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripst.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.multiuse.symbol_ref_funcs import get_symbol_stats
    from scripts.dev.workbooks_fib.fib_comb_data import add_sec_days_until_10q
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions
    from multiuse.path_helpers import get_most_recent_fpath
    from multiuse.symbol_ref_funcs import get_symbol_stats
    from workbooks_fib.fib_comb_data import add_sec_days_until_10q
    from api import serverAPI

# %% codecell
from workbooks_fib.fib_funcs import read_clean_combined_all
importlib.reload(sys.modules['workbooks_fib.fib_funcs'])
from workbooks_fib.fib_funcs import read_clean_combined_all


from multiuse.pd_funcs import mask, chained_isin
pd.DataFrame.mask = mask
pd.DataFrame.chained_isin = chained_isin
pd.set_option('display.max_columns', 30)



from talib.abstract import *
Function('RSI').info
# %% codecell

# Percentage difference for all rows in fibonacci confirmed
path_con_all = Path(baseDir().path, 'studies/fibonacci', 'confirmed_all.parquet')
df_diff_all = pd.read_parquet(path_con_all).drop_duplicates()

# Only keep stocks where the 30 day avg is > 250,000
# This keeps 2500

# %% codecell

path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals.parquet')
fib_df = pd.read_parquet(path)

fib_df[fib_df['symbol'] == 'TSLA']

# fib_df
fib_df[fib_df['symbol'] == 'SWK']

# %% codecell

# %% codecell
df_stats = get_symbol_stats()
df_stats['avg30Vol/Mil'] = (df_stats['avg30Volume'] / 1000000).round(2)
# df_large = df_stats[df_stats['avg30Vol/Mil'] > .25].copy()
df_stats.drop(columns=['date'], inplace=True)

# Get all rows of historical data
df_all = read_clean_combined_all(local=False)

df_all_mod = df_all[df_all['date'] == df_all['date'].max()][['symbol', 'fClose']].copy()
df_stats_mod = df_stats[['symbol', 'marketcap']]
df_comb_temp = pd.merge(df_stats_mod, df_all_mod, on=['symbol'], how='inner')
df_comb_temp['sharesOutstanding'] = df_comb_temp['marketcap'].div(df_comb_temp['fClose'])
df_all = pd.merge(df_all, df_comb_temp[['symbol', 'sharesOutstanding', 'marketcap']], on=['symbol'], how='left')

# %% codecell

# %% codecell


# %% codecell

# %% codecell

df_all['fourWC'] = np.where(
    (
      (df_all['symbol'] == df_all['prev_symbol'])
      & (df_all['fChangeP'] > .01)
      & (df_all['fClose'] > df_all['fClose'].shift(-1, axis=0))
      & (df_all['fClose'].shift(1, axis=0) > df_all['fClose'].shift(2, axis=0))
      & (df_all['fClose'].shift(2, axis=0) > df_all['fClose'].shift(3, axis=0))
      & (df_all['fClose'].shift(3, axis=0) > df_all['fClose'].shift(4, axis=0))
      & (df_all['fClose'].shift(4, axis=0) > df_all['fClose'].shift(5, axis=0))
    ), (df_all.index - 5), 0
)

holidays_fpath = Path(baseDir().path, 'ref_data/holidays.parquet')
holidays = pd.read_parquet(holidays_fpath)
dt = getDate.query('sec_master')
current_holidays = (holidays[(holidays['date'].dt.year >= dt.year) &
                             (holidays['date'].dt.date <= dt)])
hol_list = current_holidays['date'].dt.date.tolist()

df_four = df_all[(df_all['fourWC'] != 0)].copy()
df_four.insert(0, 'prevSymDate', df_four['date'].shift(1, axis=0))
df_four.insert(3, 'prevSymSub', df_four['symbol'].shift(1, axis=0))
cols_to_keep = ['prevSymDate', 'date', 'symbol', 'prevSymSub', 'fourWC']
df_four_sub = df_four[df_four['symbol'] == df_four['prevSymSub']][cols_to_keep].copy()

(df_four_sub.insert(2, "dayDiff",
 df_four_sub.apply(lambda row:
     np.busday_count(row['prevSymDate'].date(),
                     row['date'].date(),
                     holidays=hol_list),
                     axis=1)))

# %% codecell

# Probably need to merge back into df_four
df_four_mod = pd.merge(df_four, df_four_sub[['dayDiff']], left_index=True, right_index=True, how='left')

(df_four_mod.insert(1, 'start_date',
 df_four_mod.apply(lambda row: df_all.loc[row['fourWC']]['date'],
                   axis=1)))

syms_to_check = df_four_sub[df_four_sub['dayDiff'] <= 10]['symbol'].unique().tolist()
df_lr = df_four_mod[df_four_mod['symbol'].isin(syms_to_check)].copy()

df_lr['combRow'] = np.where(
    (
        (df_lr['dayDiff'] <= 10)
        & (df_lr['fClose'] > df_lr['fClose'].shift(1, axis=0))
    ), df_lr['start_date'].shift(1, axis=0), df_lr['start_date']
)

df_lr.loc[:, 'start_date'] = np.where(
    (
        (df_lr['start_date'] != df_lr['combRow'])
        & (df_lr['combRow'] < df_lr['date'])
    ), df_lr['combRow'], None
)

cols_to_drop = ['prevSymDate', 'prevSymSub', 'combRow']
df_lr = df_lr[~df_lr['start_date'].isna()].drop(columns=cols_to_drop).copy()
df_lr['start_date'] = pd.to_datetime(df_lr['start_date'], unit='ns')
(df_lr.insert(3, "date_range",
 df_lr.apply(lambda row:
     np.busday_count(row['start_date'].date(),
                     row['date'].date(),
                     holidays=hol_list),
                     axis=1)))

# %% codecell

# This all works perfectly for COKE and sucks for everything else.
# I can run it into the fib_sequence and see what happens.


# Need to record ext_end condition that was used. Changes the formula
# used to calculate success/failure
importlib.reload(sys.modules['workbooks.fib_funcs'])
from workbooks.fib_funcs import get_fib_dict, get_rows
df_sym = df_all[df_all['symbol'] == 'COKE']

df_lr[df_lr['symbol'] == 'COKE']
max_row = df_lr[df_lr.index == 285026].copy()

max_row = df_all[df_all.index == 285022].copy()

rows = get_rows(df_sym, max_row)
rows
max_row['cond'] = 'fiveUp'
rows = df_all[(df_all['symbol'] == 'COKE') & ((df_all['date'] >= max_row['start_date'].iloc[0]) & (df_all['date'] <= max_row['date'].iloc[0]))]

fib_dict = get_fib_dict(df_sym, max_row, rows, verb=False)


# What makes COKE different?
# The volume isn't even close to the highest
# fHigh is max high for the YTD at that point
# but also one of the lowest overall (not helpful in the moment)
# RSI is over 70 for most of that run

# %% codecell

# BIOX should be in the df_lr. Where did it go?

df_all_rsi = df_all[['symbol', 'date', 'rsi', 'marketcap']]
rsi_merge = pd.merge(fib_df[['symbol', 'date']], df_all_rsi, on=['symbol', 'date'], how='inner')
fib_syms = fib_df['symbol'].unique().tolist()
rsi_merge.drop_duplicates(subset=['symbol'], inplace=True)


fib_df = fib_df[fib_df['symbol'].isin(rsi_merge['symbol'].unique())].copy()
fib_df.insert(3, 'rsi', rsi_merge['rsi'].values)
fib_df.insert(4, 'marketcap', rsi_merge['marketcap'].values)
fib_df['marketcap'] = fib_df['marketcap'].div(1000000).round(0)

# fib_df.drop(columns=['marketcap'], inplace=True)
fib_df[fib_df['symbol'] == 'COKE']
fib_df[fib_df['rsi'] < 30]['cond'].value_counts()
fib_df[fib_df['rsi'] > 70]['cond'].value_counts()
# Less than 5 fClose's below the 78% retracement for range>start
# COND3 is wrong
fib_df[fib_df['cond'] == 'cond3']

df_all['fCP5'] = (np.where(df_all['symbol'] == df_all['prev_symbol'],
                  df_all['fChangeP'].rolling(min_periods=1, window=5).sum(),
                  0))

# %% codecell

# %% codecell
bpath = Path(baseDir().path, 'ml_data', 'fib_analysis')
fib_df_path = bpath.joinpath('fib_df_temp.parquet')
df_all_path = bpath.joinpath('df_all_temp.parquet')

write_to_parquet(fib_df, fib_df_path)
write_to_parquet(df_all, df_all_path)

# %% codecell
# Probably want to replace NaNs with 1
df_all.mask('symbol', 'COKE').chained_isin('fCP5', nlargest=20, idx_diff=True).mask('idx_diff', [-1, -2], notin=True)

df_all.mask('symbol', 'COKE').chained_isin('fCP5', nlargest=20, idx_diff=True)

# The other thing I can do is feed the data into my algorithm, which then
# makes trading decisions based off of it
# %% codecell

df_corr = df_all.corr()
df_corr
df_corr['fChangeP']

df_all.mask('symbol', 'COKE')['up50'].sum()
df_all.mask('symbol', 'OCGN')['up50'].sum()

df_all
# %% codecell


fib_df[fib_df['ext_cond'] == 'range>start'].head(50)


fib_df.columns

ret_cols = [col for col in fib_df.columns if 'ret' in str(col)]
cols_to_merge = ['symbol', 'cond', 'fibPercRange', 'start_date', 'end_date', 'date_range'] + ret_cols

df_mod = pd.merge(df_all, fib_df[cols_to_merge], left_on=['symbol', 'date'], right_on['symbol'])

# Other idea
# Can also check for continuously declining/inclining 50/200 MA
# %% codecell
fib_df_sym = fib_df.set_index('symbol')
df_all_sym = df_all.set_index('symbol')
sym_list = df_all['symbol'].unique().tolist()


for sym in tqdm(sym_list):
    df_sym = df_all_sym.loc[sym]
    fib_sym = fib_df_sym.loc[sym]



bpath = Path(baseDir().path, 'ml_data/fib_analysis')
fib_all_path = bpath.joinpath('fib_all_cleaned_data.parquet')

df_test = pd.read_parquet(fib_all_path)
cols_to_check = ['beta', 'filing', 'days_until']

# Can't use beta calc - and it doesn't even make sense here
from datetime import date
dt = date(2021, 1, 1)

df_all = read_clean_combined_all(dt=dt)
df_all = add_sec_days_until_10q(df_all).copy()

pre_cleaned_path = bpath.joinpath('pre_cleaned_data.parquet')
df_pre = pd.read_parquet(pre_cleaned_path)
df_pre.columns

path_all_updated_cleaned = bpath.joinpath('df_all_updated_cleaned.parquet')


df_all.columns

# %% codecell

df_lr[df_lr['symbol'] == 'AIRT']

df_lr.head(50)

# df_four_mod[(df_four_mod['symbol'].isin(vc[(vc > 1)].index.tolist()))][(df_four_mod['dayDiff'] <= 10) | (df_four_mod['symbol'] != df_four_mod['prevSymSub'])][cols_to_view].dropna(subset=['dayDiff'])
# df_four_mod[(df_four_mod['dayDiff'] <= 10) | ((df_four_mod['symbol'] != df_four_mod['prevSymSub']) & (df_four_mod['symbol'] == df_four_mod['symbol'].shift(-1, axis=0)))][cols_to_view]


# %% codecell


df_all = add_sec_days_until_10q(df_all)

df_fy = df_all

# df_comb = pd.merge(df_diff_all, df_large, on=['symbol'])
df_comb = pd.merge(df_large, df_fy, on=['symbol'])
df_comb['sharesOutstanding'] = df_comb['marketcap'].div(df_comb['fClose']).round(3)
df_comb['perc/shares'] = (df_comb['fVolume'].div(df_comb['sharesOutstanding'])).round(2)

df_all = pd.merge(df_comb[['symbol', 'marketcap', 'sharesOutstanding']], df_all, on=['symbol'], how='right')
# %% codecell


# Shares outstanding from IEX is wrong
# BBIG has 137,083,339 shares outstanding from Nov. 22nd 10Q
df_stats.columns
# Shares outstanding is actually the stockholders equity
# As of November 22, 2021, there were 137,083,339 shares of the registrant’s common stock outstanding.

# Remove any symbols that don't have a full year of data
"""
full_year_cond = ((df_all['symbol'].isin(df_all['symbol'].tolist())) &
                  (df_all['date'] == df_all['date'].min()))
fy_syms = df_all[full_year_cond]['symbol'].unique().tolist()
df_fy = df_all[df_all['symbol'].isin(fy_syms)].copy()
"""

# %% codecell
# Read fibonacci
path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals.parquet')
fib_df = pd.read_parquet(path)


fib_df

fib_hist_df = pd.merge(fib_df, df_comb[['symbol', 'date', 'perc/shares', 'beta', 'days_until']], on=['symbol', 'date'])

# %% codecell
# Read max_rows
path = Path(baseDir().path, 'studies/fibonacci', 'mrow_options.parquet')
mrow_df = pd.read_parquet(path)


# For GENI - row volume is top 3 in ytd
# fHigh highest for at least the last 3 months
# Significant gap - this is something I haven't explored yet
# I've seen just how useful gaps can be

# I'll have to do a gap analysis beforehand, like the pct_change
# Volume is 5x higher than the suggested date
# And the date is a full 2 months earlier

# The only conclusion I can come up with is that the symbol changed? Maybe?
# But still what the fuck, I thought I solved this problem. Fucking unbelievable
# So now this means I need the yoptions hist to fill in all dates
# That don't have the full year
# It's also possible that this isn't in cs


# These all check out. Sure there may be other trades here. But so far so good
# Now I'd like to see the correlation between

# %% codecell

# Get the 1 month past correlaton - get also ytd pct perf
# And get the high perf
df_all['cumPerc'] = np.where(
    df_all['symbol'] == df_all['prev_symbol'],
    df_all['fChangeP'].cumsum(),
    np.NaN)

df_all['perc_2weeks'] = np.where(
    df_all['symbol'] == df_all['prev_symbol'],
    df_all['fChangeP'] + df_all['fChangeP'].shift(-10),
    np.NaN)

df_all['perc_1month'] = np.where(
    df_all['symbol'] == df_all['prev_symbol'],
    df_all['fChangeP'] + df_all['fChangeP'].shift(-30),
    np.NaN)


df_all['vol_avg_2m'] = np.where(
    df_all['symbol'] == df_all['prev_symbol'],
    df_all['fVolume'].rolling(60).mean(),
    np.NaN)

df_all['vol_avg_2m'].fillna(method='bfill', inplace=True)
df_all['vol_avg_2m'] = df_all['vol_avg_2m'].round(2)

group_2m_max_vol = df_all[['symbol', 'vol_avg_2m']].groupby(by=['symbol']).max()
syms_over_hundred = group_2m_max_vol[group_2m_max_vol['vol_avg_2m'] > 100000].index.tolist()

df_100 = df_all[df_all['symbol'].isin(syms_over_hundred)].copy()

cols_to_sub = (['symbol', 'cond', 'start_date', 'end_date', 'start', 'high',
                'ext_end', 'range', 'fibPercRange', 'beta', 'ext_1.618',
                'ext_2.618', 'ext_4.236'])

fib_hist_sub = fib_hist_df[cols_to_sub].copy()
fib_hist_sub.rename(columns={'range': 'fib_range'}, inplace=True)
df_hist_sub = (pd.merge(fib_hist_sub, df_100, how='right',
                        left_on=['symbol'],
                        right_on=['symbol']))

cols_to_round = (['start', 'high', 'ext_end', 'fib_range', 'fibPercRange',
                  'beta', 'fOpen', 'fHigh', 'fLow', 'fClose', 'vol/mil',
                  'prev_close', 'fHighMax'])
df_hist_sub[cols_to_round] = (df_hist_sub[cols_to_round].astype(np.float64)
                                                        .round(2))
# %% codecell
# perc_2weeks, perc_2months can be the response variables
# Can make another response variable for whether the symbol hits the ext_2.618
# %% codecell



# %% codecell
path = Path(baseDir().path, 'ml_data/fib_analysis', 'pre_cleaned_data.parquet')
write_to_parquet(df_hist_sub, path)

# df_hist_sub.head()

# %% codecell
# %% codecell

# cols_to_exclude = ['hit_1.618', 'hit_4.236']


df_pre.head()

df_pre.info()

# %% codecell

# %% codecell

# fib_hist_df[fib_hist_df['days_until'].between(-10, 5)].sort_values(by=['days_until'], ascending=False).head(10)

# I'd like to see how many days the calculated ext_end is less than fLow (start)
# AIRI - should have picked up the fHigh, not necessarily the highest volume
# ANIP is just completely wrong - should be in may, which predicts the move later in the year
# These dates are far too late in the year
# UAVS - completely wrong - I really need to see what condition it picked and why

# Clustering algorithm - I need the first one of the year in most cases

# %% codecell

# cond1 should be highest % mover
# higher date ranges should be favored
# which means this needs to be calculated at the beginning, not the end

# Another idea is to calculate the max % change based on the run up
# AND the earliest run up because that's what's being used to
# predict the rest

# fHigh == fHigh min
# fdate == fdate min
# fVolume == fVolume.top5
# fPerc == fPerc.top5

# Xela I'm not a huge fan of
# Need to come up with other conditions as well, not just condition 2

# Can't be on a downtrend, should be on an uptrend
# Volume should be in the top 5 for the year

# The longer the date range, the more accurate the move/pullback is
# So I should focus on maximizing the date range honestly

# AXTI - ADNT wrong

# I'd like to see a consistent increase in volume. At least several times
# The 30 day average from before, for at least 5 candles

top_50_beta = fib_hist_df[fib_hist_df['ext_end'] < fib_hist_df['start']].sort_values(by='beta', ascending=False).head(50)

# Avoid investing in companies that are set to be acquired
fib_hist_df['date_range'].value_counts()


# %% codecell
fib_day5 = fib_hist_df[fib_hist_df['date_range'] > 4].sort_values(by=['beta'], ascending=False)


higher_than_786 = []
for index, row in tqdm(fib_day5.iterrows()):
    comb_row = df_comb[(df_comb['symbol'] == row['symbol']) & (df_comb['date'] > row['end_date'])]
    # if (comb_row['fClose'].min() > row['ret_0.5']):
    higher_than_786.append(row['symbol'])

# %% codecell
fib_row['ext_date'].iloc[0].date()

return_list = []
return_dict = {}

for sym in higher_than_786:
    fib_row = fib_day5[fib_day5['symbol'] == sym]
    comb_row = df_comb[(df_comb['symbol'] == sym) & (df_comb['date'] == fib_row['ext_date'].iloc[0])]
    pct_change = (((df_comb.iloc[comb_row.index + 5]['fClose'].iloc[0]) - df_comb.iloc[comb_row.index + 1]['fClose']) / df_comb.iloc[comb_row.index + 1]['fClose'])
    return_list.append(pct_change.iloc[0])
    return_dict[sym] = pct_change.iloc[0]

df_comb.iloc[comb_row.index + 1]['fClose'].iloc[0]
avg = sum(return_list)/len(return_list)
avg
min(return_list)
max(return_list)

min(return_dict.items())
return_dict

neg_returns = [n for n in return_list if n < 0]
neg_avg = sum(neg_returns)/len(neg_returns)
neg_avg

fib_hist_df[fib_hist_df['symbol'] == 'CEI']
fib_hist_df.sort_values(by=['date_range'], ascending=False).head(50)
fib_hist_df.head()

path = Path(baseDir().path, 'studies/fibonacci', 'top_50_beta.parquet')
write_to_parquet(top_50_beta, path)

# %% codecell



# %% codecell





# I'd like to see all the predictions I made where the 100% level is fHigh
# up until that point. And find the earliest high point where we exceeded
# that 100% level.

# Also - how long to retrace back to the different levels
# And how many pos candles hitting levels

# I still need to find the earliest occurence, rather than the biggest ocurrence
# If the selected value isn't in the top 5 highest (up until that point)
# that's a problem
# I should also record which ext category each symbol falls into

# I'd love to know the proximity between start dates and corporate events
# Days since most recent 10Q. Days until earnings comes out.
# I can get a list of all corporate events simply by looking at the SEC filings

# I can also take the series of rows iwth the highest avg volume


df_after = pd.merge(df_all, fib_df[['symbol', 'end_date', 'ret_0.236']], on=['symbol'], how='left')
df_after = df_after[df_after['date'] >= df_after['end_date']]
df_after_group = df_after[df_after['fClose'] < df_after['ret_0.236']][['symbol', 'date']]
df_ret_cross = df_after_group.groupby(by=['symbol'], as_index=False).min().rename(columns={'date': 'ret_cross'})
df_ret_cross = pd.merge(df_ret_cross, fib_df[['symbol', 'end_date']], on=['symbol'])

df_ret_cross['bdays_diff'] = df_ret_cross.apply(lambda row: len(pd.bdate_range(row['end_date'], row['ret_cross'])), axis=1)

# Mean for .786 was 24 days - before a full 75% retracement


df_ret_cross['bdays_diff'].describe()

df_ret_cross[df_ret_cross['bdays_diff'] < 24]
df_ret_cross.sort_values(by=['bdays_diff'], ascending=False).head(25)

fib_df[fib_df['symbol'] == 'RMBS']
fib_df.sort_values(by=['perc/shares'], ascending=False).head(50)




# %% codecell

# AMC is an interesting case. You have that initial move in January
# That moves ends up predicting the subsequent move in June
# Or at the very least defines the price-pivot levels

# I need to define clusters - for each of these, and run the analysis that way
# So I should be able to input this into my function and say that there
# were two clusters of dates
# %% codecell
from sklearn.neighbors import NearestNeighbors
from sklearn.cluster import AffinityPropagation, KMeans

model = AffinityPropagation(damping=0.9)
model = KMeans(n_clusters=2)
# model.fit(X)

mrow_df['doy'] = mrow_df['date'].dt.day_of_year
X = mrow_df[mrow_df['symbol'] == 'AMC']['doy']
nbrs = NearestNeighbors(n_neighbors=2, algorithm='auto').fit(X)
distances, indices = nbrs.kneighbors(X)
# %% codecell

# %% codecell

mrow_df[mrow_df['symbol'] == 'AMC']

mrow_df[mrow_df['symbol'] == 'OCGN']
# %% codecell

# Shares outstanding = I'd like to see a signif
df_large['sharesOutstanding']
df_large.columns


# %% codecell
# advertools
# OpenAI - personal use - tldr - keywords - text complete - facebook prophet - ludwig

df_comb.sort_values(by=['beta'], ascending=False)[['symbol', 'beta']].drop_duplicates()


df_comb[df_comb['symbol'] == 'MARA'].shape
# Length of fib_percs == 10
# Between ext and retracement and columns
# 40 combinations each, 80 possible combos for each day of data

# %% codecell
sym_vc = df_comb['symbol'].value_counts().reset_index().rename(columns={'symbol': 'sym_counts', 'index': 'symbol'})
df_mod = df_comb[['symbol', 'date']]

sym_dates = df_mod.groupby('symbol')['date'].unique().reset_index()
sym_dates['dt_count'] = sym_dates.apply(lambda row: len(row['date']), axis=1)

sym_vc = pd.merge(sym_vc, sym_dates[['symbol', 'dt_count']])
sym_vc['sym_pos'] = sym_vc['dt_count'] * 80
sym_vc['sym_ratio'] = sym_vc['sym_counts'].div(sym_vc['sym_pos'])
sym_vc = pd.merge(sym_vc, df_comb[['symbol', 'beta']].drop_duplicates(), on=['symbol'])
sym_vc.sort_values(by=['beta', 'sym_ratio'], ascending=False).head(25)

# %% codecell
# AMC Jan 25th Fibonacci extensions - predict that move up to $70
# The question is how to identify the fibonacci extensions


# Consider changing start to fHigh of the lowest day (same start date)
# MRIN is interesting. Before the big move upward, volume increases by 15x.

fib_df[fib_df['symbol'] == 'CLSK']

fib_df[fib_df['symbol'] == 'SHW']

df_comb.info()
df_all.info()


df_stats.columns
df_stats.info()

dt_vc = stats['date'].value_counts().reset_index()
dt_to_use = stats.sort_values(by=['date'], ascending=True)['date'].iloc[0]








# %% codecell









# %% codecell
