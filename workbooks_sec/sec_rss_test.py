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
