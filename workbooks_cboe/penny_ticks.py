"""Analyzing Penny tick reports from CBOE."""
# %% codecell
from pathlib import Path

import requests
import pandas as pd
import numpy as np

from io import BytesIO

try:
    from scripts.dev.multiuse.help_class import getDate
except ModuleNotFoundError:
    from multiuse.help_class import getDate

# %% codecell
# https://www.cboe.com/us/options/market_statistics/historical_data/download/class/?reportType=volume&volumeType=adv&volumeAggType=weekly
# &symbolType=underlying&symbol=OCGN&startDate=2021-10-01&endDate=2021-11-11&exchanges=CBOE&exchanges=EDGX&exchanges=C2&exchanges=BATS
# I'd like to see what the penny tick reports are, and whether
# there's any actionable data in this.
# This may be similar to the market maker reports, where it's the
# last 5 days of data

# Also not sure if I need reference data.


def get_url_dict(dt=None):
    """Get url dict of penny tick reports."""
    if dt is None:
        dt = getDate.query('iex_close')
    url_base = f"https://www.cboe.com/us/options/market_statistics/penny_tick_type/{dt.year}/{dt.month}"
    url_suf = f"_options_rpt_penny_tick_type_{dt.strftime('%Y%m%d')}.csv-dl?mkt="

    markets_dict = ({'cone': 'cone', 'bzx': 'opt',
                     'ctwo': 'ctwo', 'edgx': 'exo'})
    url_dict = {}

    for key, val in markets_dict.items():
        url_dict[key] = f"{url_base}/{key}{url_suf}{val}"

    return url_dict


def get_combine_penny_reports():
    """Get all penny reports and combine them, return df_all."""
    df_list = []
    for key, val in url_dict.items():
        try:
            get = requests.get(val)
            if get.status_code == 200:
                df = pd.read_csv(BytesIO(get.content))
                df['exch'] = key
                df_list.append(df)
            else:
                print(get.url)
        except Exception as e:
            print(e)

    df_all = pd.concat(df_list)

# %% codecell
# Should probably also get the date that's used, so I can story locally
# Or at the least use as a class variable
url_dict = get_url_dict()
df_all = get_combine_penny_reports()

# %% codecell
df_all['Tick Type'].value_counts()

df_all.info()

# %% codecell
df['Tick Type'].value_counts()

# %% codecell
