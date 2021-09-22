"""Workbook for analyzing historical prices."""
# %% codecell
################################################
from findiff import FinDiff
from scipy.signal import find_peaks
from studies.bu_candles import bu_prints
import numpy as np
import sys
import importlib
from studies.be_candles import be_prints
import os
import json
import pandas as pd
from pandas.tseries.offsets import BusinessDay
import requests

from io import BytesIO, StringIO
import gzip

from multiuse.help_class import check_size

from datetime import datetime, date, timedelta

from api import serverAPI

# %% codecell
################################################


def iex_hist_dump():
    """Get iex historical dump from server."""
    fpath = "/Users/unknown1/Algo/data/StockEOD/combined/2021-08-30.gz"

    url = "https://algotrading.ventures/api/v1/data/hist/test"
    get = requests.get(url)
    get_decomp = gzip.decompress(get.content)
    get_json = get_decomp.decode('utf-8')
    df = pd.read_json(get_json)

    dt = date.today() - timedelta(days=1)
    fpath_base = "/Users/unknown1/Algo/data/StockEOD/combined"
    fpath = f"{fpath_base}/{dt}.gz"

    df.to_json(fpath, compression='gzip')

    cols_to_keep = (['date', 'fOpen', 'fHigh', 'fClose', 'fLow',
                     'fVolume', 'change', 'changePercent', 'changeOverTime',
                     'marketChangeOverTime', 'symbol'])

    df_keep = df[cols_to_keep]
    fpath = "/Users/unknown1/Algo/data/StockEOD/combined/_cols_2021-08-30.gz"
    df_keep.to_json(fpath, compression='gzip')


fpath = "/Users/unknown1/Algo/data/StockEOD/combined/_cols_2021-08-30.gz"
df = pd.read_json(fpath, compression='gzip')

df.shape


"""
dates = df['date'].unique().tolist()
dates = pd.to_datetime(dates)

sorted_dates = sorted(dates)

# So this works, but what I really want to find is any 3 day period
date_list = []
date_max = datetime.strptime('2021-08-30', '%Y-%m-%d').date()

date_list.append(date_max)
date_list.append((date_max - BusinessDay(n=1)).date())
date_list.append((date_max - BusinessDay(n=2)).date())

df_mod = df[df['date'].isin(date_list)].copy(deep=True)
df_mod['fRange'] = (df_mod['fHigh'] - df_mod['fLow']).round(3)
df_mod.shape

df_mod_sort = df_mod.sort_values(by=['symbol', 'date'], ascending=True)
df_mod_sort = df_mod_sort[(df_mod_sort['fClose'] > 1.5) & (
    df_mod_sort['fVolume'] > 500000)].copy(deep=True)
"""

# I'm trying to take the row values for each date and turn that into their own column.

# Why don't we start with single day data.

importlib.reload(sys.modules['studies.be_candles'])
importlib.reload(sys.modules['studies.bu_candles'])
pd.options.display.max_rows = 100


# %% codecell
###################################################

# from studies.bu_candles import bu_prints

fpath = "/Users/unknown1/Algo/data/StockEOD/combined/_cols_2021-08-30.gz"
df = pd.read_json(fpath, compression='gzip')


def analyze_hist_candles(df, bull=True, bear=False):
    """Analyze historical candlestick patterns."""

    # Get list of all symbols from server
    all_symbols = serverAPI('all_symbols').df
    # Keep only common stocks for analysis
    all_cs = all_symbols[all_symbols['type'] == 'cs']['symbol'].tolist()

    # Eliminate prices < $1.5, and volume less than 500,000
    df_sorted = df[(df['fClose'] > 1.5) & (
        df['fVolume'] > 500000)].copy(deep=True)
    # Keep only common stocks
    df_sorted = df_sorted[df_sorted['symbol'].isin(all_cs)]
    # Add a range column
    df_sorted['fRange'] = (df_sorted['fHigh'] - df_sorted['fLow']).round(3)
    # Add a duplicate symbol column for analysis
    df_sorted['sym'] = df_sorted['symbol']

    # Sort by symbol and date, descending
    df_sorted.sort_values(by=['symbol', 'date'], ascending=True, inplace=True)

    if bull:
        bu_prints(df_sorted.set_index('symbol'))
    if bear:
        be_prints(df_sorted.set_index('symbol'))

    return df_sorted


# %% codecell
###################################################

df_sorted = analyze_hist_candles(df).copy(deep=True)

# Bullish morning star doji - check volume for that first candle
# One could make the argument that there should be higher than avg vol
# For at least one of the candles


bu_prints(df_sorted.set_index('symbol'))

# %% codecell
###################################################

%load_ext zipline

df_test
df_test.head(100)
# Increase tail size of 3 black crows

be_prints(df_sorted.set_index('symbol'))


# %% codecell
################################################

fpath = "/Users/unknown1/Algo/data/sec/zip_files/q1_2021_all.csv"
df = pd.read_csv(fpath)


df.head(10)
# Round the closing prices.

df_sorted['fCloseR'] = df_sorted['fClose'].astype('uint16')
df_aapl = df_sorted[df_sorted['symbol'] == 'AAPL']



def trend_naive(hist):
    """Trend identification: naive method."""
    hs = hist.Close.loc[hist.Close.shift(-1) != hist.Close]
    x = hs.rolling(window=3, center=True).aggregate(lambda x: x[0] > x[1] and x[2] > x[1])
    minimaIdxs = [hist.index.get_loc(y) for y in x[x == 1].index]
    x = hs.rolling(window=3, center=True).aggregate(lambda x: x[0] < x[1] and x[2] < x[1])
    maximaIdxs = [hist.index.get_loc(y) for y in x[x == 1].index]


def trend_num_diff(df):
    """Trend identification numerical differentiation."""
df['fCloseR'] = df['fClose'].round()

dx = 1 #1 day interval
d_dx = FinDiff(0, dx, 1)
d2_dx2 = FinDiff(0, dx, 2)
clarr = np.asarray(df_sorted['fCloseR'])
mom = d_dx(clarr)
momacc = d2_dx2(clarr)

mom

df_sorted['num_mom'] = mom
df_sorted['num_momacc'] = momacc
h = df_sorted['fCloseR'].tolist()


cols_to_look = ['fClose', 'num_mom', 'num_momacc', 'minimaNum', 'maximaNum']
df_sorted[cols_to_look].head(25)

def get_extrema(isMin):
    return [x for x in range(len(mom))
    if (momacc[x] > 0 if isMin else momacc[x] < 0) and
      (mom[x] == 0 or #slope is 0
        (x != len(mom) - 1 and #check next day
          (mom[x] > 0 and mom[x+1] < 0 and
           h[x] >= h[x+1] or
           mom[x] < 0 and mom[x+1] > 0 and
           h[x] <= h[x+1]) or
         x != 0 and #check prior day
          (mom[x-1] > 0 and mom[x] < 0 and
           h[x-1] < h[x] or
           mom[x-1] < 0 and mom[x] > 0 and
           h[x-1] > h[x])))]
minimaIdxs, maximaIdxs = get_extrema(True), get_extrema(False)

df_sorted['minimaNum'] = 0
df_sorted['minimaNum'] = np.where(df_sorted.index.isin(minimaIdxs), 1, 0)


df_sorted['maximaNum'] = 0
df_sorted['maximaNum'] = np.where(df_sorted.index.isin(maximaIdxs), 1, 0)

len(mom)
df_sorted.head(10)

ymin, ymax = [h[x] for x in minimaIdxs], [h[x] for x in maximaIdxs]
zmin, zmne, _, _, _ = np.polyfit(minimaIdxs, ymin, 1, full=True)


pmin = np.poly1d(zmin).c
zmax, zmxe, _, _, _ = np.polyfit(maximaIdxs, ymax, 1, full=True) #y=zmax[0]*x+zmax[1]
pmax = np.poly1d(zmax).c
print((pmin, pmax, zmne, zmxe))


decimals = 0
# df_sorted['fCloseR'] = df_sorted['fCloseR'].apply(lambda x: round(x, decimals))
df_sorted['fCloseR'] = df_sorted['fClose'].round()


from findiff import FinDiff #pip install findiff
dx = 1 #1 day interval
d_dx = FinDiff(0, dx, 1)
d2_dx2 = FinDiff(0, dx, 2)
clarr = np.asarray(df_sorted['fCloseR'])
mom = d_dx(clarr)
momacc = d2_dx2(clarr)

def get_extrema(isMin):
  return [x for x in range(len(mom))
    if (momacc[x] > 0 if isMin else momacc[x] < 0) and
      (mom[x] == 0 or #slope is 0
        (x != len(mom) - 1 and #check next day
          (mom[x] > 0 and mom[x+1] < 0 and
           h[x] >= h[x+1] or
           mom[x] < 0 and mom[x+1] > 0 and
           h[x] <= h[x+1]) or
         x != 0 and #check prior day
          (mom[x-1] > 0 and mom[x] < 0 and
           h[x-1] < h[x] or
           mom[x-1] < 0 and mom[x] > 0 and
           h[x-1] > h[x])))]
minimaIdxs, maximaIdxs = get_extrema(True), get_extrema(False)



# Mathematical formula for ascending triangle
# First let's start with the stock highs - at least 2 of them forming a horizontal line.
# So take the 2 highest points in a 30 day period. See if you can draw a line between them.
# Consider each day as a 1 increase on the x coordinate plane

# This doesn't need to be done to get my algorithm working, but I would like to see this happen.


df__all = df_sorted[df_sorted['symbol'] == 'ALL'].copy(deep=True)
df__all.sort_values(by=['date'], ascending=True, inplace=True)
df__all.shape
peaks, _ = find_peaks(df__all['fHigh'], distance=15)
_
peaks

for pos in peaks:
    df__all.iloc[pos, df__all.columns.get_loc('peaks')] = 1


dx = 1
d_dx = FinDiff(0, dx, 1)
d2_dx2 = FinDiff(0, dx, 2)
clarr = np.asarray(df__all['fClose'])
mom = d_dx(clarr)
momacc = d2_dx2(clarr)


import trendln


# %% codecell
################################################
