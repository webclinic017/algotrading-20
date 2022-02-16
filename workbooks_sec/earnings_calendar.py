"""Earnings calendar/earnings analysis."""
# %% codecell
from pathlib import Path
import sys
import importlib
import os

import pandas as pd
from pandas.tseries.offsets import BusinessDay
import numpy as np
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.multiuse.path_helpers import path_or_yr_direct
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from multiuse.path_helpers import path_or_yr_direct
    from data_collect.iex_class import urlData
    from api import serverAPI


# %% codecell

from ref_data.symbol_meta_stats import SymbolRefMetaInfo
importlib.reload(sys.modules['ref_data.symbol_meta_stats'])
from ref_data.symbol_meta_stats import SymbolRefMetaInfo

# %% codecell

srmi = SymbolRefMetaInfo()
symbol_ref = srmi.df.reset_index()
symbol_ref = symbol_ref[['symbol', 'sector', 'industry']]
# from multiuse.fpaths import fpaths_dict
# %% codecell
df_all = srmi.df.dropna(subset=['sector', 'industry'])

# First step. Second step is to check for past earnings accouncements
# For that, I need the SEC daily_index. Keep only the last 10Q

sec_all = serverAPI('sec_master_all').df
sec_all['date'] = pd.to_datetime(sec_all['date'], unit='ms')
sec_all['Form Type'] = sec_all['Form Type'].astype('category')

sec_all['date'].max()

# %% codecell

from data_collect.analyst_earnings_ests import ScrapedEE
importlib.reload(sys.modules['data_collect.analyst_earnings_ests'])
from data_collect.analyst_earnings_ests import ScrapedEE

importlib.reload(sys.modules['multiuse.path_helpers'])
from multiuse.path_helpers import get_most_recent_fpath, path_or_yr_direct

from api import serverAPI
importlib.reload(sys.modules['api'])

from time import sleep
# %% codecell


mr_fpath = get_most_recent_fpath(fpath_dir)
dt_prev = getDate.query('iex_previous')

see_prev = ScrapedEE()
df_prev = see_prev.df.copy()


# serverAPI('redo', val='combine_daily_stock_eod')
anal_ests = serverAPI('analyst_ests_all').df
df_ests = anal_ests.set_index('symbol')
df_ests['marketCap'] = df_ests['marketCap'].astype(np.int64)

stat_cols = ['sector', 'industry', 'beta', 'day30ChangePercent', 'year1ChangePercent']
df_stats = df_all[stat_cols].copy()

df_comb = df_ests.join(df_stats)

df_comb['date'] = pd.to_datetime(df_comb['date'])

df_comb['adjDate'] = np.where(
    df_comb['time'] == 'time-after-hours',
    df_comb['date'] + BusinessDay(n=1),
    df_comb['date']

)





from datetime import date
from workbooks_fib.fib_funcs import read_clean_combined_all
dt_start = date(2022, 1, 1,)
df_hist = read_clean_combined_all(dt=dt_start)

hist_cols = ['symbol', 'date', 'fChangeP', 'fRange', 'perc5']
df_hist_mod = df_hist[hist_cols].copy()
df_hist_mod.rename(columns={'date': 'adjDate'}, inplace=True)

df_hist_mod.columns

df_mod = pd.merge(df_comb.reset_index(), df_hist_mod, on=['symbol', 'adjDate'], how='left')
dt = getDate.query('iex_close')

cond_pre1 = (df_mod['date'].dt.date <= dt)
cond_pre2 = (df_mod['time'] != 'time-not-supplied')
df_pre = df_mod[cond_pre1 & cond_pre2].copy()


group_cols = ['sector', 'marketCap', 'beta', 'fChangeP', 'perc5']
df_pre['marketCap'] = df_pre['marketCap'] / 1000000

df_pre[group_cols].groupby('sector').mean().dropna(subset=['fChangeP']).sort_values(by=['fChangeP'])


df_hist.columns

# serverAPI('redo', val='combine_analyst_earnings_estimates')
# %% codecell


# %% codecell


# p.stat().st_size

# So the entire point of this is just to get the earnings data
# To see if the company did well or not
# And then see the corresponding reaction in the market
# url = 'https://www.sec.gov/files/company_tickers.json'


# Get all quarterly reports
df_10q = sec_all[sec_all['Form Type'].str.contains('10-Q')]

all_symbols = serverAPI('all_symbols').df
all_syms = all_symbols[['symbol', 'cik']].copy()

df_10qt = pd.merge(all_syms, df_10q, on=['cik'])
df_10qt = pd.merge(df_10qt, symbol_ref, on='symbol')

url_pre = 'https://www.sec.gov/Archives/'

hon_10qt = df_10qt[df_10qt['symbol'] == 'HON']
url = f"{url_pre}edgar/data/320193/0000320193-21-000065.txt"
url

headers = ({'User-Agent': 'Rogue Technology Ventures edward@rogue.golf',
            'Referer': 'https://www.sec.gov/structureddata/rss-feeds-submitted-filings',
            'Host': 'www.sec.gov',
            'Accept-Encoding': 'gzip, deflate',
            'Cache-Control': 'no-cache',
            'Accept-Language': 'en-GB,en;q=0.5'})

get = requests.get(url, headers=headers)
if get.status_code != 200:
    print(f"{str(get.status_code)} {get.url}")


# %% codecell
test_content = get.content
# %% codecell


url_mid = f"edgar/data/{url_suf.split('/')[2]}/"
url_suf1 = '0000773840-21-000065.hdr'

url_t1 = f"{url_pre}{url_mid}{url_suf1}"
url_t1

url_s1 = 'edgar/data/0000773840/773840-21-000065.hdr'
url_t2 = f"{url_pre}{url_s1}"
url
get_t1 = requests.get(url_t2, headers=headers)

get_t1.status_code
get_t1.content

soup = BeautifulSoup(get.content.decode('utf-8'), "html.parser")

soup.fine_all('')

url1 = 'https://data.sec.gov/submissions/CIK0000320193.json'

header_json = headers.copy()
header_json['Host'] = 'data.sec.gov'

get = requests.get(url1, headers=header_json)
get_hon.status_code

df_aapl = pd.DataFrame(get.json()['filings']['recent'])

df_aapl
df_aapl['form'].value_counts()

df_aapl_10q = df_aapl[df_aapl['form'] == '10-Q']
df_10qt[df_10qt['symbol'] == 'AAPL']

aapl-20211225.htm

# https://www.sec.gov/Archives/edgar/data/1811856/000119312521079857/d155143d8k.htm

url_test1 = 'https://www.sec.gov/Archives/edgar/data/0000320193/000032019322000007/aapl-20211225.htm'
get = requests.get(url_test1, headers=headers)
get.status_code

soup1 = BeautifulSoup(get.content.decode('utf-8'), "html.parser")

print(soup1.prettify())

df_aapl_10q


# %% codecell

# Let's get the 1 day return, 2 day return, 5 day return
# After hours - still the same day, won't make a shit of difference
# That morning - sure, counts for something.

# If there's a way to pull timestamp data off the filing, that'd be great

edgar/data/876523/0000876523-22-000019.txt


df_10qt.sort_values(by=['date'], ascending=False)
# %% codecell
fdir = Path('/Users/eddyt/Algo/data/sec/2021q4')
df_pre = pd.read_csv(fdir.joinpath('pre.txt'), sep='\t')

df_sub = pd.read_csv(fdir.joinpath('sub.txt'), sep='\t')

df_sub.columns

df_num = pd.read_csv(fdir.joinpath('num.txt'), sep='\t')



df_num

df_pre


# %% codecell


df_10qt

sec_all['Form Type'].value_counts().head(25)

sec_all['date'].max()

# For derivatives, I'd like to see Avg daily volume to at least something reasonable, or a decent spread

# I can probably just stick with the ~ 5,000 stocks for now.
df_all['nextEarningsDate'].value_counts()

srmi.df


# %% codecell



# %% codecell


# Two options - will probably end up using both
# 1. Get meta data and use that
# 2. Get earnings calendar data from IEX

# %% codecell
