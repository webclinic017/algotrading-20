# %% codecell
##########################################

import yfinance as yf
import pandas as pd
import requests
from tqdm import tqdm
from pathlib import Path
from datetime import date
from multiuse.help_class import baseDir, getDate, df_create_bins
from multiuse.create_file_struct import make_hist_prices_dir

from data_collect.iex_class import urlData
importlib.reload(sys.modules['data_collect.iex_class'])
from data_collect.iex_class import urlData

import importlib
import sys

# %% codecell
##########################################

# Make hist prices file struct
# fpath = Path(baseDir().path, 'derivatives', 'end_of_day')
# make_hist_prices_dir(fpath)
# %% codecell

symbol = 'OCGN'

from dotenv import load_dotenv
import os

load_dotenv()
base_url = os.environ.get("base_url")
url_suf = '/ref-data/options/symbols'
payload = {'token': os.environ.get("iex_publish_api")}
get = requests.get(f"{base_url}{url_suf}", params=payload)
get_json = get.json()
sym_list = pd.DataFrame(get_json.keys())
sym_list.columns = ['symbol']
df_syms = df_create_bins(sym_list)
bins = df_syms['bins'].unique().to_numpy()


def get_nord_proxies():
    """Get proxy list from nord vpn."""
    nord_url = 'https://api.nordvpn.com/v1/servers/recommendations'
    get = requests.get(nord_url)
    nord_df = pd.DataFrame(get.json())
    nord_df['port'] = nord_df['hostname'].str[2:6]
    # Get nord password
    nord_pass = os.environ.get("nord_pass")
    if not nord_pass:
        nord_pass = 'YNGWqBf2zHaLSV6'
    purl = f"edwardtomasso@gmail.com:{nord_pass}@"
    nord_df['p_test'] = nord_df.apply(lambda row: {
                                        'https': f"https://{row.station}:{row.port}"},
                                        axis=1)
    nord_df['proxies'] = nord_df.apply(lambda row:
                                       {'http': f"{purl}{row.hostname}",
                                        'https': f"{purl}{row.hostname}"},
                                       axis=1)
    nord_df['socks5'] = nord_df.apply(lambda row:
                                       {'http': f"socks5://{purl}{row.hostname}",
                                        'https': f"socks5://{purl}{row.hostname}"},
                                       axis=1)
    nord_df['socks5p'] = nord_df.apply(lambda row:
                                       {'http': f"socks5://{purl}{row.hostname}:{row.port}",
                                        'https': f"socks5://{purl}{row.hostname}:{row.port}"},
                                       axis=1)

    return nord_df

nord_df = get_nord_proxies()

# %% codecell


# %% codecell



# %% codecell

nord_url = 'https://nordvpn.com/api/server'
get = requests.get(nord_url)

server_list = pd.DataFrame(get.json())
col_list = list(server_list['features'].iloc[0].keys())
server_list[col_list] = server_list.features.apply(lambda row: pd.Series(row))
socks_df = server_list[server_list['socks'] == True].copy()

nord_pass = 'YNGWqBf2zHaLSV6'
purl = f"edwardtomasso@gmail.com:{nord_pass}@"
socks_df['socksp'] = socks_df.apply(lambda row:
                                   {'http': f"socks5://{purl}{row.domain}:1080",
                                    'https': f"socks5://{purl}{row.domain}:1080"},
                                   axis=1)


nord_df['p_test'].iloc[0]

nord_df['p_test'].iloc[10]
# %% codecell
int(sym_list.shape[0] / (socks_df.shape[0] - 1))


# %% codecell

bin_df = pd.DataFrame(columns=['bins', 'proxy'])
for bin, row in zip(bins, socks_df['socksp']):
    bin_df = bin_df.append({'bins': bin, 'proxy': row}, ignore_index=True)

df_comb = pd.merge(df_syms, bin_df, on='bins')

proxy_list = socks_df['socksp'].tolist()
n = 0
p_n = 0  # Proxy number
proxy = proxy_list[p_n]

for index, row in tqdm(df_comb.iterrows()):
    n = yahoo_options(row['symbol'], proxy=proxy, n=n)
    if n > 1990:
        proxy = proxy_list[(p_n + 1)]
        print('Changing proxies')
        n = 0

# %% codecell
error_list = []
empty_list = []

def yahoo_options(sym, proxy=False, n=False):
    """Get options chain data from yahoo finance."""
    dt = getDate.query('iex_eod')
    yr = dt.year
    fpath_base = Path(baseDir().path, 'derivatives/end_of_day', str(yr))
    fpath = Path(fpath_base, sym.lower()[0], f"_{sym}.parquet")

    if fpath.is_file():
        df_old = pd.read_parquet(fpath)

    ticker = yf.Ticker(sym)
    exp_dates = ticker.options
    if n:
        n += (len(exp_dates) + 1) * 2
    df_list = []

    for exp in exp_dates:
        try:
            if proxy:
                options = ticker.option_chain(exp, proxy=proxy)
            else:
                options = ticker.option_chain(exp)
            df_calls = options.calls
            df_calls['side'] = 'C'
            df_puts = options.puts
            df_puts['side'] = 'P'
            df_list.append(df_calls)
            df_list.append(df_puts)
        except:
            error_list.append(sym)
            break

    try:
        df_all = pd.concat(df_list)

        df_all['date'] = pd.to_datetime(dt)
        df_all['symbol'] = sym
        df_all = df_all.set_index('symbol')

        if fpath.is_file():
            df_old = pd.read_parquet(fpath)
            df_all = pd.concat([df_old, df_all])
            df_all.to_parquet(fpath)
        else:
            df_all.to_parquet(fpath)

    except ValueError:
        empty_list.append(sym)

    return n
# %% codecell


# %% codecell
##########################################
import logging
import threading
import time
import concurrent.futures

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(messae)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(thread_function, range(3))



# %% codecell
import logging
import threading
import time
import concurrent.futures

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

from tqdm import tqdm
def call_yahoo_options(df):
    """Call yahoo options with the right proxy."""
    # print(df)
    for index, row in tqdm(df.iterrows()):
        # print([row['symbol'], row['proxy']])
        try:
            yahoo_options(row['symbol'], proxy=row['proxy'])
        except Exception as e:
            print(e)
        # break


arg_list = ['flower', 'petal', 'creature']
args = [df_comb[df_comb['bins'] == n] for n in iter(bins)]

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    args = [df_comb[df_comb['bins'] == n] for n in iter(bins)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(call_yahoo_options, iter(args))


# %% codecell
bin_size = int(sym_df.shape[0] / (len(proxies) - 1))
df_syms = df_create_bins(sym_df, bin_size=bin_size)
bins = df_syms['bins'].unique().to_numpy()

bin_df = pd.DataFrame(columns=['bins', 'proxy'])
for bin, row in zip(bins, proxies):
    bin_df = bin_df.append({'bins': bin, 'proxy': row}, ignore_index=True)

df_comb = pd.merge(df_syms, bin_df, on='bins')

# %% codecell

# %% codecell
