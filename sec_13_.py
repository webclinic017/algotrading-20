"""Workbook for institutional ownership changes."""
# %% codecell
#####################################################
import time
import os.path, os
import importlib
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, time

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import bs4.element
from charset_normalizer import CharsetNormalizerMatches as CnM

from data_collect.sec_routines import secInsiderTrans, secMasterIdx
importlib.reload(sys.modules['data_collect.sec_routines'])
from data_collect.sec_routines import secInsiderTrans, secMasterIdx

from data_collect.sec_form13s import get13F

from multiuse.help_class import baseDir, dataTypes

from multiuse.sec_helpers import add_ciks_to_13FHRs

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
#####################################################
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

sec_df_np = sec_df[sec_df['Form Type'].str.contains('PORT', regex=True)].copy(deep=True)
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
from multiuse.help_class import getDate
import datetime
dt = getDate.query('sec_master')
dt = dt.strftime('%Y%m%d')
dt_test = datetime.datetime.strptime(dt, '%Y%m%d')
dt = 'none'
url = f"https://algotrading.ventures/api/v1/sec/master_idx/date/most_recent"
get = requests.get(url)

df = pd.DataFrame(get.json())

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

df = serverAPI('sec_master_mr', val='most_recent').df
df['Form Type'].value_counts()


df = serverAPI('sec_inst_holdings').df
df.shape
df.head(10)


from datetime import date
from pandas.tseries.offsets import BusinessDay
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
from multiuse.help_class import dataTypes

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
