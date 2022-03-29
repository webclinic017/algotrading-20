# %% codecell
from bs4 import BeautifulSoup

# %% codecell
# df_cleaned1.info()

try:
    url = 'http://www.crunchbase.com/organization/slack'
    headers = ({'Accept-Encoding': 'gzip, deflate, br', 'Cache-Control': 'no-cache', 'Host': 'www.crunchbase.com',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:92.0) Gecko/20100101 Firefox/92.0'})
    get = requests.get(url, headers=headers)
except Exception as e:
    print(e)


get.status_code
# %% codecell
bs = BeautifulSoup(get.text)

tag = bs.b

# %% codecell
from pathlib import Path

import tabula
import pandas as pd

from multiuse.help_class import baseDir, write_to_parquet
from api import serverAPI

# %% codecell
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 50)
# %% codecell

fpath = '/Users/eddyt/Downloads/US3000_QUARTERLY-DailyData-USD_StocksWeight_20210930.pdf'
list_all = tabula.read_pdf(fpath, pages='all')
df = (pd.concat(list_all)
        .rename(columns={'Russell 3000®': 'name'})
        .drop_duplicates(subset=['name'])
        .reset_index(drop=True))

fpath = Path(baseDir().path, 'funds', '_russell3000.parquet')

all_symbols = serverAPI('all_symbols').df
all_symbols['name']


df
# %% codecell
