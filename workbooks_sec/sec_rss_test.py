"""Sec RSS tests."""
# %% codecell

from pathlib import Path
from datetime import datetime, date, timedelta

import importlib
import sys
import pandas as pd
import numpy as np


from data_collect.sec.sec_rss import SecRssFeed, AnalyzeSecRss
importlib.reload(sys.modules['data_collect.sec.sec_rss'])
from data_collect.sec.sec_rss import SecRssFeed, AnalyzeSecRss

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

from telegram.methods.push import telegram_push_message
importlib.reload(sys.modules['telegram.methods.push'])
from telegram.methods.push import telegram_push_message

from multiuse.help_class import getDate, baseDir, write_to_parquet
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import getDate, baseDir, write_to_parquet

from telegram.methods.push import telegram_push_message

import logging
logging.basicConfig(level=logging.DEBUG)
# %% codecell

serverAPI('redo', val='sec_rss_task')

# %% codecell
bdir = Path(baseDir().path, 'social', 'telegram', 'sec')
fpath = bdir.joinpath('_sec_rss_sent.parquet')
df_smsgs = pd.read_parquet(fpath)

tz = 'US/Eastern'
pd.to_datetime(df_smsgs['dt'], tz)

df_smsgs

# %% codecell

kwargs = {'verbose': True, 'testing': True}
srf = SecRssFeed(**kwargs)
srf.df['pubDate'].max()
srf.df


kwargs = {'verbose': True, 'testing': False}
asr = AnalyzeSecRss(**kwargs)


# %% codecell

sec_ref = serverAPI('sec_ref').df
# Get all symbols (IEX ref)
all_syms = serverAPI('all_symbols').df
all_syms.drop_duplicates(subset='cik', inplace=True)
df_ref = (sec_ref.merge(
          all_syms[['symbol', 'cik', 'type']],  on='cik'))

df_latest = serverAPI('sec_rss_latest').df
df_latest[['guid', 'description']].value_counts()
df_latest['guid'].value_counts()

df_latest['dt'].max()
df_latest['pubDate'].max()

df_latest


sec_all = df.merge(df_ref, on='cik')





# %% codecell

dft = pd.DataFrame()

stamp = pd.Timestamp(datetime.now())
dft['stamp'] = [stamp, stamp]

dft['stamp'].apply(lambda x: x.value)

dir(dft['stamp'].dt)

value

dir(dft['stamp'])
stamp.to_pydatetime().

stamp.isoformat()
dir(stamp)
utcfromtimestamp()
dir(stamp.to_pydatetime())

# %% codecell
fpath = '/Users/eddyt/Downloads/_2022-03-10.parquet'
dft = pd.read_parquet(fpath)


infer_dtype(dft['dt'].dtype)

dft.drop_duplicates('guid')

latest = serverAPI('sec_rss_latest').df
df_rss = latest.drop_duplicates('guid')
f_test =
from multiuse.fpaths import FindTheFpath

ff = FindTheFpath(category='sec', keyword='latest')
# We can hopefully assume at this point that the df_msgs_today is not empty

dt = getDate.query('iex_eod')
fpath = Path(baseDir().path, 'sec', 'rss', '2022', f"_{dt}.parquet")

write_to_parquet(df_rss, fpath)

ff.options
ff.cat_dict
dir(ff)
# %% codecell

# %% codecell



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
