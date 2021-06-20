"""Workbook for getting options data from IEX Cloud."""

# %% codecell
########################################################
import os
from datetime import date
import string

import pandas as pd
import requests
from dotenv import load_dotenv

from multiuse.help_class import baseDir, dataTypes, getDate

from api import serverAPI

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
########################################################
import glob
import threading
base_dir = baseDir().path
fpath = f"{base_dir}/derivatives/iex_symref/**"
paths = glob.glob(fpath, recursive=True)


view_symref = IexOptionSymref('VIEW')

all_syms = serverAPI('all_symbols').df
all_cs = all_syms[all_syms['type'] == 'cs']
all_cs.shape
all_cs_sym = all_cs['symbol'].tolist()

for sym in all_cs_sym:
    th = threading.Thread(target=IexOptionSymref, args=(sym,))
    th.start()

all_syms.head(10)

def iex_options_symbol_ref():
    """Add tasks to queue to execute."""
    syms_fpath = f"{base_dir}/tickers/all_symbols.gz"
    all_syms = pd.read_json(syms_fpath, compression='gzip')

    all_cs = all_syms[all_syms['type'] == 'cs'].tolist()

# %% codecell
########################################################


class IexOptionSymref():
    """Get iex options symref data, write to json."""
    fpath = ''

    def __init__(self, sym):
        self.construct_fpath(self, sym)
        df_raw = self.get_data(self, sym)

        self.concat_data(self, df_raw)
        self.write_to_json(self)

    @classmethod
    def construct_fpath(cls, self, sym):
        """Construct local fpath."""
        base_dir, yr = baseDir().path, date.today().year
        fpath_base = f"{base_dir}/derivatives/iex_symref/{yr}"
        fpath = f"{fpath_base}/{sym.lower()[0]}/_{sym}.gz"
        self.fpath = fpath

    @classmethod
    def get_data(cls, self, sym):
        """Construct parameters."""
        load_dotenv()
        base_url = os.environ.get("base_url")
        payload = {'token': os.environ.get("iex_publish_api")}
        url = f"{base_url}/ref-data/options/symbols/{sym}"

        get = requests.get(url, params=payload)
        df = pd.DataFrame(get.json())
        return df

    @classmethod
    def concat_data(cls, self, df):
        """Concat data with previous file if it exists."""

        if os.path.isfile(self.fpath):
            df_all = pd.concat([pd.read_json(self.fpath), df])
            df_all = (df_all.drop_duplicates(subset='symbol')
                            .reset_index(drop=True))
            df = dataTypes(df_all).df.copy(deep=True)
        else:
            # Minimize size of data
            df = dataTypes(df).df.copy(deep=True)

        self.df = df

    @classmethod
    def write_to_json(cls, self):
        """Write data to local json file."""
        self.df.to_json(self.fpath, compression='gzip')


# %% codecell
########################################################

base_path = f"{baseDir().path}/derivatives/iex_symref"










# %% codecell
########################################################
