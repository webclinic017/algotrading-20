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

from data_collect.sec_routines import get_cik, secInsiderTrans, secMasterIdx
importlib.reload(sys.modules['data_collect.sec_routines'])
from data_collect.sec_routines import get_cik, secInsiderTrans, secMasterIdx

from multiuse.help_class import baseDir

from multiuse.bs4_funcs import bs4_child_values

from data_collect.sec_form13s import get13F
importlib.reload(sys.modules['data_collect.sec_form13s'])
from data_collect.sec_form13s import get13F

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
#####################################################
# Form 13G 13G/A 13D/A

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

sec_url = "https://www.sec.gov/Archives"
url_fname = row_test['File Name']

url = f"{sec_url}/{url_fname}"
get = requests.get(url)

soup = BeautifulSoup(get.content.decode('utf-8'))

col_dict = {''}


tbls = soup.find_all("table")

val_list = []
for tbl in tbls:
    for val in tbl.descendants:
        if isinstance(val, bs4.element.NavigableString):
            if val not in ('\n', ''):
                val_list.append(val)
        else:
            pass

tr_list = []
for tbl in tbls:
    tr_list.append(tbl.find_all("tr"))



tr_list

trs = soup.find_all("table")

len(val_list)
val_ser = pd.Series(val_list).str.strip()
val_ser = val_ser.str.replace('\n', ' ')

possible_cols =
val_ser.value_counts()
list(soup.find_all("p", text="  Name of reporting persons."))


# %% codecell
#####################################################

# overview_df = pd.DataFrame(tag_dict, index=range(1))
# print(CnM.from_bytes(get.content[0:10000]).best().first())

# %% codecell
#####################################################

print(CnM.from_bytes(get.content).best().first())


# %% codecell
#####################################################
