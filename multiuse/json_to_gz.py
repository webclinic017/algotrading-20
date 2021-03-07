"""Convert local files saved in json to .gz."""
# %% codecell
###############################################
import os
import glob
from datetime import date
from copy import deepcopy

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

# %% codecell
###############################################


def convert_json_to_gz():
    """Convert local json files to .gz."""
    # Get fpaths for all local syms
    glob_fpath = f"{baseDir().path}/StockEOD/{date.today().year}/*/**"
    local_stock_data = glob.glob(glob_fpath)
    local_stock_data = sorted(local_stock_data)

    local_syms = []  # Create an empty list
    for st in local_stock_data:  # Split strings and store symbol names
        local_syms.append(st.split('_')[1])

    local_syms_dict = {}
    for sym, path in zip(local_syms, local_stock_data):
        local_syms_dict[sym] = pd.read_json(path)
        local_syms_dict[sym].drop_duplicates(subset=['date'], inplace=True)

    local_stock_gz = deepcopy(local_stock_data)
    local_stock_gz = sorted([f"{path}.gz" for path in local_stock_gz])

    for sym, path in zip(local_syms_dict, local_stock_gz):
        local_syms_dict[sym].to_json(path, compression='gzip')

    # Remove original json files
    for st in local_stock_data:
        if '.gz' not in st:
            os.remove(st)

# %% codecell
###############################################
