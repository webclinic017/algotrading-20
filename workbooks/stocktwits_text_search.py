"""Ideas for searching through text."""
# %% codecell
"""
Come up with a list of all dates - month and day
Ex. Nov 9
Ex. November 9th
Ex. November 9

Consider the above one in the same
Used the library Dave recommended from his friend to
search through all of this and compile a list of possible
important dates (upcoming)

Keyword frequency will be really interesting.
I can use the Nordvpn proxies to exceed the rate threshold per minute/hour

"""
import pandas as pd
import numpy as np
from pathlib import Path
import requests
from tqdm import tqdm

from multiuse.api_helpers import get_sock5_nord_proxies
from multiuse.help_class import getDate, dataTypes, baseDir, write_to_parquet
from api import serverAPI

from datetime import date
# %% codecell

url = 'https://stocktwits.com/symbol/OCGN'
test = pd.read_html(url, attrs={'class': 'infinite-scroll-component '})

# %% codecell


def clean_st_messages(df):
    """Clean st data."""
    cols_to_drop = ['entities', 'links']
    for col in cols_to_drop:
        try:
            df.drop(columns=[col], inplace=True)
        except KeyError:
            pass

    for col in df.columns:
        try:
            if col in ['mentioned_users']:
                df[col] = df[col].astype('str')
            elif col in ['user_classification']:
                print(str(df[col]))
            elif isinstance(df[col].dropna().iloc[0], list):
                col_dict = df[col].explode()
                df_col = pd.DataFrame.from_records(col_dict.dropna().reset_index(drop=True)).copy()
                df_col.columns = [f"{col}_{key}" for key in df_col.columns.tolist()]
                df = pd.concat([df, df_col], axis=1).copy()
                df.drop(columns=[col], inplace=True)
            elif isinstance(df[col].dropna().iloc[0], dict):
                df_col = pd.DataFrame.from_records(df[col].dropna().reset_index(drop=True)).copy()
                df_col.columns = [f"{col}_{key}" for key in df_col.columns.tolist()]
                df = pd.concat([df, df_col], axis=1).copy()
                df.drop(columns=[col], inplace=True)
        except IndexError:
            print(col)

    try:
        df.drop(columns=['symbols_aliases'], inplace=True)
    except KeyError:
        pass
    return df

# %% codecell

all_symbols = serverAPI('all_symbols').df
all_cs = all_symbols[all_symbols['type'].isin(['cs', 'ad'])]
symbol_list = all_cs['symbol'].tolist()

result = get_sock5_nord_proxies()
# %% codecell

syms_collected = []
syms_needed = list(set(symbol_list) - set(syms_collected))
dt = date.today()
verb = True

for proxy in result:
    syms_needed = list(set(symbol_list) - set(syms_collected))
    s = requests.Session()
    s.proxies.update(proxy)
    for symbol in tqdm(syms_needed):

        url_1 = 'https://api.stocktwits.com/api/2/streams'
        url_2 = f'/symbol/{symbol}.json'
        url = f"{url_1}{url_2}"

        try:
            get = s.get(url)
        except ConnectionError:
            break

        if get.status_code == 200:

            df = pd.DataFrame(get.json()['messages'])
            df = clean_st_messages(df)

            path = Path(baseDir().path, 'all_symbol_data', f"{symbol}", 'daily',  f"_{dt}.parquet")
            if path.exists():
                df_old = pd.read_parquet(path)
                df_all = pd.concat([df_old, df])
                df_all.drop_duplicates(subset=['id'])
            else:
                df_all = df.copy()
            df_all = df_all.dropna().reset_index(drop=True)
            try:
                write_to_parquet(df_all, path)
                syms_collected.append(symbol)
            except Exception as e:
                print(f"Could not write symbol {symbol} to parquet: {str(e)}")
        elif get.status_code == 404:
            if get.json()['errors'][0]['message']:
                syms_collected.append(symbol)
            continue
        else:
            if verb:
                print(symbol, get.status_code, get.json()['errors'])
            break

# %% codecell

# %% codecell
