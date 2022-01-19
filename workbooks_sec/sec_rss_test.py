"""Sec RSS tests."""
# %% codecell

from pathlib import Path
from datetime import datetime, date, timedelta

import importlib
import sys
import pandas as pd
import numpy as np


from data_collect.sec_rss import SecRssFeed, AnalyzeSecRss
importlib.reload(sys.modules['data_collect.sec_rss'])
from data_collect.sec_rss import SecRssFeed, AnalyzeSecRss

from api import serverAPI

# %% codecell

srf = SecRssFeed()
srf_df = srf.df

all_syms = serverAPI('all_symbols').df
ocgn_df = all_syms[all_syms['symbol'] == 'OCGN']

srf_df.info()

srf_df['dt'] = pd.to_datetime(srf_df['pubDate'])

prev_15 = (datetime.now() - timedelta(minutes=60)).time()
sec_df = (srf_df[(srf_df['dt'].dt.time > prev_15)
          & (srf_df['dt'].dt.date == date.today())]
          .copy())


sec_df

srf_df[srf_df['CIK'] == ocgn_df['cik'].iloc[0]]
srf_df.df
# %% codecell

latest = serverAPI('sec_rss_latest').df

# %% codecell

latest['description'].value_counts()

sec_all = serverAPI('sec_rss_all').df

# %% codecell
import requests

url = 'https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK=&type=4&owner=include&count=100&action=getcurrent'
headers = ({'User-Agent': 'Rogue Technology Ventures edward@rogue.golf',
            'Referer': 'https://www.sec.gov/structureddata/rss-feeds-submitted-filings',
            'Host': 'www.sec.gov',
            'Accept-Encoding': 'gzip, deflate',
            'Cache-Control': 'no-cache',
            'Accept-Language': 'en-GB,en;q=0.5'})
get = requests.get(url, headers=headers)

url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=4&company=&dateb=&owner=include&start=0&count=100&output=atom'
df = pd.read_xml(get.content).loc[6:]


df = pd.concat([df, df['title'].str.split('-', expand=True)], axis=1)
df.rename(columns={0: 'form', 1: 'pre_split'}, inplace=True)
df_test = pd.concat([df, df['pre_split'].str.split(r'[()]', n=3, expand=True)], axis=1)
cols_to_drop = ['rel', 'href', 'name', 'email', 'link', 'category', 'pre_split', 2]
df_test.drop(columns=cols_to_drop, inplace=True)

cols_to_rename = {0: 'person/entity', 1: 'p/e/#', 3: 'p/e/desc'}
df_test.rename(columns=cols_to_rename, inplace=True)

cols_to_rename = {1: 'date', 3: 'accNo'}

df_mod = df_test['summary'].str.split(expand=True).rename(columns=cols_to_rename)
df_mod['size'] = df_mod[[5, 6]].agg(''.join, axis=1)
df_semi = pd.concat([df_test, df_mod[['date', 'accNo', 'size']]], axis=1)
df_semi['p/e/desc'] = df_semi['p/e/desc'].str.replace(')', '', regex=False)

df_semi.drop(columns=['id', 'summary'], inplace=True)

df_semi['form'] = df_semi['form'].str.replace(' ', '')
df_semi = df_semi[df_semi['form'] == '4'].copy()
df_semi['updated'] = pd.to_datetime(df_semi['updated'], infer_datetime_format=True)
reordered_cols = (['person/entity', 'form', 'date', 'p/e/desc',
                   'p/e/#', 'accNo', 'size', 'updated', 'title'])
df_semi = df_semi[reordered_cols].reset_index(drop=True)
df_final = df_semi.copy()

df_final




# %% codecell




# %% codecell
