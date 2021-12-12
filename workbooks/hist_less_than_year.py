"""Getting missing hist data from less than a year."""
# %% codecell

from pathlib import Path
from datetime import datetime, date, timedelta

from tqdm import tqdm
import requests
import pandas as pd
import numpy as np

from io import BytesIO
import importlib
import sys

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, dataTypes, write_to_parquet
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, dataTypes, write_to_parquet
    from workbooks.fib_funcs import read_clean_combined_all
    from missing_data.get_missing_hist_from_yf import get_yf_loop_missing_hist
    from api import serverAPI
# %% codecell

from workbooks.fib_funcs import read_clean_combined_all
importlib.reload(sys.modules['workbooks.fib_funcs'])
from workbooks.fib_funcs import read_clean_combined_all

df_all = read_clean_combined_all(local=True)
df_all['date'] = pd.to_datetime(df_all['date'])
df_2021 = df_all[df_all['date'].dt.year == 2021].copy(deep=True)
# %% codecell

# %% codecell
# get_yf_loop_missing_hist(key='get_ignore_ytd')
all_syms = serverAPI('all_symbols').df
all_syms.columns


# %% codecell
dt = getDate.query('iex_previous')
dt
url1 = 'ftp://ftp.nyxdata.com/'
url2 = 'cts_summary_files'
url3 = f"CTA.summary.{dt.strftime('%Y%m%d')}"

url = f"{url1}{url2}"
get = requests.get()


import shutil
import urllib.request as request
from contextlib import closing
from urllib.error import URLError

file = url3
file = 'CTA.Summary.CXE.20211124.csv'
try:
    with closing(request.urlopen(url)) as r:
        with open(file, 'wb') as f:
            shutil.copyfileobj(r, f)
except URLError as e:
    if e.reason.find('No such file or directory') >= 0:
        raise Exception('FileNotFound')
    else:
        raise Exception(f'Something else happened. "{e.reason}"')

# %% codecell
# %% codecell

/metadata/group/{group name}/name/{dataset name}
url = 'https://api.finra.org/data/group/otcMarket/name/blocksSummary'
headers = {'Accept': 'application/json'}
get = requests.get(url, headers=headers)

from io import BytesIO

df = pd.DataFrame(get.json())
df.info()

pd.set_option('display.max_columns', 500)
df
# %% codecell
url = 'https://www.cboe.com/us/equities/market_statistics/distribution/2021/11/bzx_equities_distribution_rpt_20211124.txt-dl'
get = requests.get(url, headers=headers)
get.status_code
df = pd.read_csv(BytesIO(get.content))
get.text
# What was the plan here - fill in missing data?
# Except I can't remember the name of this symbol
df_2021[df_2021['symbol'] == 'GENI']
syms_one_miss = vc[(vc < (vc.max() - 1)) & (vc > 0)].index

# %% codecell
from missing_data.get_missing_hist_from_yf import get_yf_loop_missing_hist

# %% codecell
syms_one_miss.tolist()

get_yf_loop_missing_hist(syms_one)
# %% codecell
vc.index
max_data = df_all.value_counts(subset=['symbol'], ascending=False)[0]
df_all[df_all['']]
max_data
# %% codecell









# %% codecell
