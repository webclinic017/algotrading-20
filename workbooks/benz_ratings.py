"""Benz ratings webscraping."""
# %% codecell
from pathlib import Path
import secrets
import string
import random

import pandas as pd
import numpy as np

import importlib
import sys
import requests
import json
from urllib.parse import urlencode, quote_plus

from multiuse.help_class import getDate, baseDir, write_to_parquet, help_print_arg, dataTypes
from data_collect.iex_class import urlData
from workbooks_fib.fib_funcs import read_clean_combined_all
from api import serverAPI
# %% codecell
sapi = serverAPI('redo', val='bz_recs')
# %% codecell

from data_collect.bz_data import BzRecs
importlib.reload(sys.modules['data_collect.bz_data'])
from data_collect.bz_data import BzRecs

# %% codecell
bz_recs = BzRecs()
# %% codecell

# Next steps for Bz recs:
# 1. Compile reputation score for each firm, for each analyst
# 1-3 day performance, 1-3 month performance, 1-3 year performance
# Time until stock hit target - if it did at all
# Time for how long the stock stayed at the target
# Time until/after most recent company announcement/ 10Q filing

# %% codecell
def bz_unfinished():
    """BZ Unfinished."""

    headers = ({'Accept': 'application/json', 'Accept-Encoding': 'gzip',
                'Connection': 'keep-alive', 'DNT': '1', 'Host': 'api.benzinga.com',
                'Origin': 'https://www.benzinga.com', 'Referer': 'https://benzinga.com',
                'Sec-Fetch-Dest': 'emtpy', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Site': 'same-site',
                'Sec-GPC': '1', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:94.0) Gecko/20100101 Firefox/94.0'})

    # token: '1c2735820e984715bc4081264135cb90'
    # token: 'anBvLgmzgKHJhQdQQzBe24yKFpHwcBJN'
    # token: ''
    # 6 letters, 2 numbers, uppercase as well
    # Eventually I'll end up guessing someone's code

    N = 32
    ran_code = ''.join(random.choices(string.ascii_lowercase + string.digits, k = N))
    sec_code = ''.join(secrets.choice(string.ascii_lowercase + string.digits) for i in range(N))

    params = {'token': sec_code, 'parameters[date_from]': '2021-11-26', 'parameters[date_to]': '2021-11-26', 'pagesize': 1000}
    # %% codecell

    url1 = 'https://api.benzinga.com'
    url2 = '/api/v2.1/calendar/ratings'
    url = f"{url1}{url2}"

    s = requests.Session()

    req = requests.Request('GET', url, params=urlencode(params, quote_via=quote_plus), headers=headers)
    prepped = req.prepare()
    prepped.url = prepped.url.replace('%5B', '[')
    prepped.url = prepped.url.replace('%5D', ']')
    prepped.url
    # %% codecell
    resp = s.send(prepped)

    df = None
    if resp.status_code == 200:
        df = pd.DataFrame.from_records(resp.json()['ratings'])
    else:
        print(f"Response status code of {resp.status_code} with text {resp.text}")

    # Url:
    # https://api.benzinga.com/api/v2.1/calendar/ratings?
    # token=1c2735820e984715bc4081264135cb90&parameters[date_from]=2021-11-26&parameters[date_to]=2021-11-26&pagesize=1000
# %% codecell

# %% codecell
fstring = 'analyst_recs'
path = Path(baseDir().path, f'company_stats/{fstring}/{fstring}.parquet')


from api import serverAPI
importlib.reload(sys.modules['api'])


sapi = serverAPI('analyst_recs_all')
df = sapi.df

df.info()

df['Rating Change'].value_counts()

df[df['symbol'] == 'OCGN']

df['np/mark'] = np.where(
    df['new_price'] > df['mark'],
    1 - df['mark'].div(df['new_price']),
    (df['mark'] - df['new_price']).div(df['mark'])
)

df.sort_values(by=['date'], ascending=True)

df
# %% codecell

# cols_to_round = ['fOpen', 'fLow', 'fClose', 'fHigh']
# df_all.dropna(subset=cols_to_round, inplace=True)
# df_all.loc[:, cols_to_round] = df_all[cols_to_round].round(3)

# I'd like to get lagged returns
# This first part is the almost identical code to the gap code
# df_all['f%_lag1'] = df_all['fClose'].shift(periods=1, axis=0)

df_comb = pd.merge(df, df_all, on=['date', 'symbol'])

df_all.head()

df_all.info()

# %% codecell

df_comb = pd.merge(df, df_all, on=['date', 'symbol'])
df_comb.insert(9, 'prevClose', df_comb['prev_close'], allow_duplicates=True)
df_comb.drop(columns=['prev_close'], inplace=True)
# %% codecell
cols_to_drop = ['mark', 'perc_change', 'fVolume']
df_comb.drop(columns=cols_to_drop, inplace=True)
consensus_cols = ['date', 'symbol', 'prev_price', 'new_price']

group_con_base = df_comb[consensus_cols].groupby(by=['date', 'symbol'])
group_con = group_con_base.mean()
group_con['conPct'] = group_con.pct_change(axis='columns', periods=1)['new_price'].values
group_con = group_con.reset_index().drop(columns=['prev_price', 'new_price']).copy()
group_con['conPct'] = group_con['conPct'].round(3)
df_comb = pd.merge(df_comb, group_con, how='left', on=['date', 'symbol'])


# codecell

# I could do the consensus price, but I have less faith in that than
# the individual analyst/firm
group_count = (df_comb[['date', 'symbol', 'Analyst Firm']].groupby(by=['date', 'symbol']).count().rename(columns={'Analyst Firm': 'firmCount'}).reset_index())
df_groupby = pd.merge(group_con, group_count, on=['date', 'symbol'])


df_up = df_comb[df_comb['prev_price'] < df_comb['new_price']].copy()
df_down = df_comb[df_comb['prev_price'] > df_comb['new_price']].copy()

group_up_base = df_up[consensus_cols].groupby(by=['date', 'symbol'])
group_down_base = df_down[consensus_cols].groupby(by=['date', 'symbol'])

group_up_max = group_up_base.max().drop(columns=['prev_price'])
group_up_min = group_up_base.min().drop(columns=['new_price'])

group_down_max = group_down_base.min().drop(columns=['new_price'])
group_down_min = group_down_base.min().drop(columns=['prev_price'])

group_ups = group_up_min.join(group_up_max)
group_downs = group_down_max.join(group_down_min)

group_extremes = pd.concat([group_ups, group_downs]).rename(columns={'prev_price': 'prevExt', 'new_price': 'newExt'})

group_extremes

group_con_base.mean()
group_con_means = group_con_base.mean().reset_index()
group_con_means

df_groupby = pd.merge(df_groupby, group_extremes.reset_index(), how='left', on=['date', 'symbol'])
df_groupby = pd.merge(df_groupby, group_con_means, on=['date', 'symbol'])



df_groupby.sort_values(by=['firmCount'], ascending=False)


df_groupby


# df_comb.sort_values(by=['gap'], ascending=False)

df_comb
# %% codecell

# %% codecell
