"""Workbook for institutional ownership changes."""
# %% codecell
#####################################################
from multiuse.help_class import dataTypes
from pandas.tseries.offsets import BusinessDay
from datetime import date
import datetime
from multiuse.help_class import getDate
from api import serverAPI
from multiuse.sec_helpers import add_ciks_to_13FHRs
from multiuse.help_class import baseDir, dataTypes, check_size
from data_collect.sec_form13s import get13F
import time
import os.path
import os
import importlib
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, time

from io import BytesIO
import gzip

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import bs4.element
from charset_normalizer import CharsetNormalizerMatches as CnM

from data_collect.sec.sec_routines import secInsiderTrans, secMasterIdx
importlib.reload(sys.modules['data_collect.sec.sec_routines'])


importlib.reload(sys.modules['api'])

from data_collect.sec.sec_rss import SecRssFeed
importlib.reload(sys.modules['data_collect.sec.sec_rss'])
from data_collect.sec.sec_rss import SecRssFeed

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 50)

# %% codecell
#####################################################

sec_rss = SecRssFeed()
sec_df = sec_rss.df

sec_df.head()
# %% codecell

sec_link = "https://www.sec.gov/opa/data/market-structure/market-structure-data-security-and-exchange.html"

sec_ind_sec_metrics_link = "https://www.sec.gov/files/opa/data/market-structure/metrics-individual-security/individual_security_2021_q1.zip"
get = requests.get(sec_ind_sec_metrics_link)

from io import BytesIO

f = BytesIO(get.content)

dir_to_extract = f"{baseDir().path}/sec/zip_files"

from zipfile import ZipFile

with ZipFile(f, 'r') as zip:
    df_contents = pd.DataFrame(zip.printdir())
    zip.extractall(dir_to_extract)

zip.infolist()
df_contents = pd.read_csv(zip.infolist())

df = pd.read_csv(get.text)
get.headers

# %% codecell
##########################################################


def clean_sec_metrics():
    """Read, clean, and return sec stock metrics data."""
    fpath = "/Users/unknown1/Algo/data/sec/zip_files/q1_2021_all.csv"
    df = pd.read_csv(fpath)
    df = dataTypes(df).df.copy(deep=True)
    df['date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df['symbol'] = df['Ticker']

    df['Cancel-to-Trade'] = (df['Cancels'] / df['LitTrades']).round(3)
    df['Trade-to-Order-Volume'] = (df["LitVol('000)"] / df["OrderVol('000)"]).round(3)
    df['Hidden-Rate'] = (df['Hidden'] / df['TradesForHidden']).round(3)
    df['Hidden-Volume'] = (df["HiddenVol('000)"] / df["TradeVolForHidden('000)"]).round(3)
    df['Oddlot-Rate'] = (df['OddLots'] / df['TradesForOddLots']).round(3)
    df['Oddlot-Volume'] = (df["OddLotVol('000)"] / df["TradeVolForOddLots('000)"]).round(3)

    cols_to_round = (["LitVol('000)", "OrderVol('000)", "HiddenVol('000)",
                      "OddLotVol('000)", "TradeVolForOddLots('000)"])
    for col in cols_to_round:
        df[col] = df[col].round(3)

    return df


def get_stock_combined_and_write():
    """Get stock eod dump, and write to local file."""
    url_base = "https://algotrading.ventures/ap1/v1"
    url_dump = f"{url_base}/data/stock_eod/dump"
    dt = getDate.query('iex_eod')

    get = requests.get(url_dump)

    df = pd.read_json(gzip.decompress(get.content))

    df = dataTypes(df).df.copy(deep=True)
    df.reset_index(drop=True, inplace=True)

    fpath = f"{baseDir().path}/StockEOD/combined/{dt}.gz"
    df.to_json(fpath, compression='gzip')

    return df

df_sec = clean_sec_metrics()


# sym_list = df['Ticker'].unique().tolist()
dt = getDate.query('iex_eod')
fpath = f"{baseDir().path}/StockEOD/combined/{dt}.gz"
df_eod = pd.read_json(fpath, compression='gzip')


df_sec['date'] = df_sec['Date']
df_sec['symbol'] = df_sec['Ticker']
df_sec['date'] = pd.to_datetime(df_sec['date'], format='%Y%m%d')

df_merged = pd.merge(df_sec, df_eod, on=['date', 'symbol'])
df_merged.reset_index(drop=True, inplace=True)
df_merged = dataTypes(df_merged).df.copy(deep=True)
df_merged['date'] = df_merged['date'].dt.date

pd.set_option('use_inf_as_na', True)

df_symbol_dummy = pd.get_dummies(df_merged['symbol'])
df_date_dummy = pd.get_dummies(df_merged['date'])
cols_to_drop = (['Date', 'Security', 'Ticker', 'label', 'id', 'key', 'subkey',
                 'symbol', 'updated', 'date', 'marketClose', "OddLotVol('000)"])
df_merged.drop(columns=cols_to_drop, inplace=True)
df_merged.dropna(inplace=True)

unadjust_cols = (['uOpen', 'uLow', 'uHigh', 'uClose', 'uVolume', 'open',
                  'close', 'low', 'high', 'volume'])
df_merged.drop(columns=unadjust_cols, inplace=True)

for col in df_merged.columns:
    print(f"{col} nans: {df_merged[col].isna().sum()}")

df_one_hot = pd.concat([df_merged, df_symbol_dummy, df_date_dummy], axis=1).copy(deep=True)
df_one_hot.dropna(inplace=True)
df_one_hot.reset_index(drop=True, inplace=True)
df_one_hot = dataTypes(df_one_hot).df.copy(deep=True)

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeRegressor

cols_float = df_one_hot.select_dtypes(include=['float32']).columns.tolist()
for col in cols_float:
    df_one_hot[col] = df_one_hot[col].astype('float64')



X = df_one_hot.drop(columns='changePercent')
y = df_one_hot['changePercent']
x_train, x_test, y_train, y_test = train_test_split(X, y, random_state=0)

lr = LinearRegression()
model = lr.fit(x_train, y_train)
model.score(x_test, y_test)


ridge = Ridge().fit(x_train, y_train)
print("Training set score: {:.2f}".format(ridge.score(x_train, y_train)))
print("Test set score: {:.2f}".format(ridge.score(x_test, y_test)))

model = lr.fit(x_train, y_train)
model.score(x_test, y_test)
dir(model)



"""
Odd lots - Ind. investors are usually wrong and more likely to generate odd-lot sales.
    If odd lots are up and small investors are selling a stock, good time to buy.
    Not persistently valid.
Round lots - Begin at 100 shares and are divisible by 100. More compelling that these
    Are professional/institutional investors.

Hidden Orders - breaks up an order into smaller pieces that buy/sell at optimal times.
    This is to (obv) hide a big order.

    Most hidden orders are placed at the last second and therefore can't get filled.
    Algo trading increased the demand for hidden orders.

    Sometimes also called iceberg orders.

Random Walk Theory - Stock prices have the same distribution and are independent of each other.
    Past movement can't predict future movement. Random/unpredictable path that makes all
    Methods of predicting stock prices futile in the long run.

    Impossible to outperform market without assuming additional risk.

"""


df_ocgn = df[df['Ticker'] == 'OCGN']

df_ocgn[df_ocgn['Date'].isin([20210202, 20210203, 20210204, 20210205, 20210208, 20210209])]

df_ocgn



df.info(memory_usage='deep')



# %% codecell
#########################################################


hazards_link = "https://www.sec.gov/files/opa/data/market-structure/hazards-and-survivors-time-period/haz_sur_q1_2021.zip"
get = requests.get(hazards_link)

f = BytesIO(get.content)

dir_to_extract = f"{baseDir().path}/sec/zip_files"

from zipfile import ZipFile

with ZipFile(f, 'r') as zip:
    zip.extractall(dir_to_extract)

# %% codecell
#########################################################

# Form 13G 13G/A 13D/A

# sec_idx = serverAPI(which='redo', val='sec_idx_master')
# sec_inst = serverAPI(which='sec_inst_holdings')

# iex_close = serverAPI(which='redo', val='iex_close')

sec_master = serverAPI(which='redo', val='sec_idx_master')
sec_master = serverAPI(which='redo', val='combine_all_sec_masters')

sec_masters = serverAPI(which='sec_master_all').df

sec_inst.df.shape
"""
OCGNs merger agreement
https://fintel.io/doc/sec-hsgx-histogenics-8k-2019-april-08-17994
"""

sec_master = secMasterIdx()
sec_df = sec_master.df.copy(deep=True)
sec_df.shape
sec_df.dtypes
sec_df['Form Type'].value_counts()

sec_df_497k = sec_df[sec_df['Form Type'] == '497K'].copy(deep=True)
sec_df_497 = sec_df[sec_df['Form Type'] == '497'].copy(deep=True)
sec_df_FWP = sec_df[sec_df['Form Type'] == 'FWP'].copy(deep=True)
sec_df_424B2 = sec_df[sec_df['Form Type'] == '424B2'].copy(deep=True)
sec_df_485BPOS = sec_df[sec_df['Form Type'] == '485BPOS'].copy(deep=True)

sec_df_np = sec_df[sec_df['Form Type'].str.contains(
    'PORT', regex=True)].copy(deep=True)
sec_df_np
"""
sec_df_13 = sec_df[sec_df['Form Type'].str.contains('13', regex=False)].copy(deep=True)
sec_df_13['Form Type'].value_counts()

# 13F-NT - No holdings, reported by other funds

# Start with the 13F-HR
sec_df_13HR = sec_df_13[sec_df_13['Form Type'] == '13F-HR'].copy(deep=True)
sec_df_13G = sec_df_13[sec_df_13['Form Type'] == 'SC 13G'].copy(deep=True)
sec_df_13D = sec_df_13[sec_df_13['Form Type'] == 'SC 13D'].copy(deep=True)
sec_df_13DA = sec_df_13[sec_df_13['Form Type'] == 'SC 13D/A'].copy(deep=True)
sec_df_13FNT = sec_df_13[sec_df_13['Form Type'] == '13F-NT'].copy(deep=True)

row = sec_df_13HR.iloc[1]
row_test = sec_df_13D.iloc[0]
row_13FNT = sec_df_13FNT.iloc[3]
row_13G = sec_df_13G.iloc[0]
row_13DA = sec_df_13DA.iloc[0]
"""

# %% codecell
#####################################################
sec_df_13G = sec_df[sec_df['Form Type'] == 'SC 13G'].copy(deep=True)
sec_df_13G.shape
row_test = sec_df_13G.iloc[10]


# %% codecell
#####################################################

all_syms = serverAPI('all_symbols').df
all_syms.head(10)


ms_all = serverAPI('sec_master_all').df


ms_all = dataTypes(ms_all).df.copy(deep=True)
ms_all.shape

ms_all.info(memory_usage='deep')

ms_all['Form Type'].value_counts()

ms_all['Date Filed'].value_counts()

f13f_hr = ms_all[ms_all['Form Type'] == '13F-HR'].copy(deep=True)

test_13f = get13F(f13f_hr.iloc[0])
test_13f.df.head(10)
f13f_hr.head(10)


f13f_hr.shape

f13f_hr['Date Filed'].value_counts()
f13f_hr.head(100)


# %% codecell
#####################################################
dt = getDate.query('sec_master')
dt = dt.strftime('%Y%m%d')
dt_test = datetime.datetime.strptime(dt, '%Y%m%d')
dt = 'none'
url = f"https://algotrading.ventures/api/v1/sec/master_idx/date/most_recent"
get = requests.get(url)

df = pd.DataFrame(get.json())

importlib.reload(sys.modules['api'])

df = serverAPI('sec_master_mr', val='most_recent').df
df['Form Type'].value_counts()


df = serverAPI('sec_inst_holdings').df
df.shape
df.head(10)


dt = (date.today() - BusinessDay(n=1)).date()

# """
url_base = "https://algotrading.ventures/api/v1/sec/master_idx/date/"
for n in list(range(15, 40)):
    dt = (date.today() - BusinessDay(n=n)).date()
    requests.get(f"{url_base}{dt.strftime('%Y%m%d')}")
    time.sleep(.5)
# """

# url = f"https://algotrading.ventures/api/v1/sec/master_idx/date/{dt.strftime('%Y%m%d')}"
# get = requests.get(url)
# overview_df = pd.DataFrame(tag_dict, index=range(1))
# print(CnM.from_bytes(get.content[0:10000]).best().first())

url = "https://algotrading.ventures/api/v1/sec/data/master_idx/all/false"
get = requests.get(url)

all_df = pd.DataFrame(get.json())
all_df = dataTypes(all_df).df

all_df['Date Filed'].value_counts()
df_13FHR = all_df[all_df['Form Type'] == '13F-HR'].copy(deep=True)
for ron, row in enumerate(df_13FHR):
    print(df_13FHR.iloc[ron])
    break
df_13FHR.head(10)

all_df['Form Type'].value_counts()

url = "https://algotrading.ventures/api/v1/sec/data/form_13FHR"
get = requests.get(url)


get.content
df = pd.DataFrame(get.json())
# %% codecell
#####################################################

ref_df = serverAPI('sec_ref').df
ref_df.shape

inst_holds_df = serverAPI('sec_inst_holdings').df

inst_holds_df['CIK'].value_counts()


# fpath = secFpaths(sym='TDAC', cat='company_idx')

# %% codecell
#####################################################

# %% codecell
#####################################################
