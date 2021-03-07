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


def convert_json_to_gz(which):
    """Convert local json files to .gz."""
    fpath_dict = ({
        'StockEOD': f"{baseDir().path}/StockEOD/{date.today().year}/*/**",
        'tickers': f"{baseDir().path}/tickers/*"
    })

    l_stock = glob.glob(fpath_dict[which])
    l_stock = [f for f in l_stock if os.path.isfile(f) if '.gz' not in f]
    l_stock = sorted(l_stock)

    if which == 'StockEOD':
        local_syms = []  # Create an empty list
        for st in l_stock:  # Split strings and store symbol names
            local_syms.append(st.split('_')[1])

        local_syms_dict = {}
        for sym, path in zip(local_syms, l_stock):
            local_syms_dict[sym] = pd.read_json(path)
            local_syms_dict[sym].drop_duplicates(subset=['date'], inplace=True)

        l_stock_gz = deepcopy(l_stock)
        l_stock_gz = sorted([f"{path}.gz" for path in l_stock_gz])

        for sym, path in zip(local_syms_dict, l_stock_gz):
            local_syms_dict[sym].to_json(path, compression='gzip')
    elif which == 'tickers':
        for file in l_stock:
            df_file = pd.read_json(file)
            mod_file = f"{file}.gz"
            df_file.to_json(mod_file, compression='gzip')
            print(file)

    # Remove original json files
    for st in l_stock:
        os.remove(st)



# %% codecell
###############################################
