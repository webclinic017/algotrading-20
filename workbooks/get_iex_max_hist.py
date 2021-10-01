"""Get IEX max historical data, check for local missing files."""
# %% codecell
from pathlib import Path
from dotenv import load_dotenv
import os
from tqdm import tqdm

import requests
import pandas as pd

from multiuse.help_class import baseDir
from api import serverAPI

# %% codecell


def find_missing_local_hist():
    """Find all symbols where local data is missing."""
    sf_fpath = Path('../../algo_jansen/data/single_factor_syms.parquet')
    sf_syms = pd.read_parquet(sf_fpath)

    hs_fpath = Path('../data/historical/2021')
    hs_fpath.resolve()
    hs_fpaths = list(hs_fpath.glob('**/*.parquet'))
    hs_list = [str(path).split('_')[1].split('.')[0] for path in hs_fpaths]

    syms_needed = list(set(sf_syms['symbols'].tolist()) - set(hs_list))

    return syms_needed


def get_all_max_hist(sym_list=False):
    """Get all max historical symbol data from IEX."""
    load_dotenv()
    base_url = os.environ.get("base_url")
    base_path = f"{baseDir().path}/historical/2021"
    true, false = True, False
    payload = {'token': os.environ.get("iex_publish_api"), 'chartByDay': true}

    if not sym_list:
        all_symbols = serverAPI('all_symbols').df
        all_syms = all_symbols[all_symbols['type'].isin(['cs'])]
        sym_list = all_syms['symbol'].tolist()

    hist_dict, hist_errors_dict = {}, {}
    hist_list, hists_checked, hist_errors = [], [], []

    for sym in tqdm(sym_list):
        fpath = f"{base_path}/{sym[0].lower()}/_{sym}.parquet"
        # If the local file doens't already. Doesn't check for missing data
        if not os.path.exists(fpath):
            url = f"{base_url}/stock/{sym}/chart/max"
            # payload = {'token': os.environ.get("iex_publish_api"), 'chartByDay': true}
            get = requests.get(url, params=payload)

            try:
                df = pd.DataFrame(get.json())
                # hist_dict[sym] = df
                hist_list.append(sym)
                df.to_parquet(fpath)
            except Exception as e:
                print(e)
                hist_errors_dict[sym] = get
                hist_errors.append(sym)
        else:
            hists_checked.append(sym)
        # break

    result = ({'hist_dict': hist_dict, 'hist_list': hist_list,
               'hists_checked': hists_checked,
               'hist_errors_dict': hist_errors_dict,
               'hist_errors': hist_errors})

    return result

syms_needed = find_missing_local_hist()
result = get_all_max_hist(syms_needed)
