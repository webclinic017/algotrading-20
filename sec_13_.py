"""Workbook for institutional ownership changes."""
# %% codecell
#####################################################
import time
import os.path, os
import importlib
import sys
import xml.etree.ElementTree as ET

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import bs4.element
from charset_normalizer import CharsetNormalizerMatches as CnM

from data_collect.sec_routines import secInsiderTrans, secMasterIdx
importlib.reload(sys.modules['data_collect.sec_routines'])
from data_collect.sec_routines import secInsiderTrans, secMasterIdx

from daily_front_funcs.scans import scansClass
importlib.reload(sys.modules['daily_front_funcs.scans'])
from daily_front_funcs.scans import scansClass

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


vol_avg = scansClass(which='vol', by='avg')
vol_avg.path
vol_avg.comb_path


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
from datetime import timedelta

# %% codecell
#####################################################


class scansClass():
    """Class to implement markets scans."""

    base_dir = baseDir().path
    df, comb_path = None, None

    def __init__(self, which, by):
        self.determine_params(self, which, by)
        if not isinstance(self.df, pd.DataFrame):
            self.call_func(self, which, by)

    @classmethod
    def determine_params(cls, self, which, by):
        """Determine parameters used for functions."""
        dt = getDate.query('iex_close')
        dt_1, path_1 = dt - timedelta(days=1), False

        fpath_dict = ({
            'vol': {
                'avg': f"{self.base_dir}/scans/top_vol/_{dt}.gz",
                'avg_1': f"{self.base_dir}/scans/top_vol/_{dt_1}.gz"
            }
        })

        path = fpath_dict[which][by]
        # If path is a file, read file and return
        if os.path.isfile(path):
            self.df = pd.read_json(path)
            return
        else:
            path_1 = fpath_dict[which][f"{by}_1"]
            self.get_comb_path(self, path, path_1, dt)

    @classmethod
    def get_comb_path(cls, self, path, path_1, dt):
        """Get iex combined path data."""
        # If one day behind is still not a file path, look for iex_combined
        if not os.path.isfile(path_1):
            for n in list(range(1, 6)):
                dt = dt - timedelta(days=n)
                comb_path = f"{self.base_dir}/iex_eod_quotes/combined/_{dt}.gz"
                if os.path.isfile(comb_path):
                    self.comb_path = comb_path
                    break
        # If IEX data from today, then today is the path. If yesterday, then _1
        if path_1:
            self.path = path_1
        else:
            self.path = path

    @classmethod
    def call_func(cls, self, which, by):
        """Determine class function to call."""
        if which == 'vol' and by == 'avg':
            self.call_vol_avg(self)

    @classmethod
    def call_vol_avg(cls, self):
        """Perform ops for highest volume/avg."""
        # Read all symbols fpath and filter to only common stock
        syms_fpath = f"{self.base_dir}/tickers/all_symbols.gz"
        all_symbols = pd.read_json(syms_fpath, compression='gzip')
        cs_syms = all_symbols[all_symbols['type'] == 'cs']['symbol'].tolist()

        # Read all iex_close data, sort to only common stock, create new column
        df = pd.read_json(self.comb_path, compression='gzip')
        df = df[df['symbol'].isin(cs_syms)].copy(deep=True)
        df['vol/avg'] = (df['volume'] / df['avgTotalVolume'] * 100).round(0)
        df['changePercent'] = (df['changePercent'] * 100).round(1)
        df['ytdChange'] = (df['ytdChange'] * 100).round(1)
        self.df = df.sort_values(by=['vol/avg'], ascending=False).head(50)
        # Write to local json file

        self.df.to_json(self.path, compression='gzip')
