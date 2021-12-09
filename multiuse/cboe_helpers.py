"""Helper functions for getting cboe data."""
# %% codecell

from pathlib import Path
from io import BytesIO

import pandas as pd
import numpy as np
import requests

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from multiuse.path_helpers import get_most_recent_fpath

# %% codecell


def cboe_symref_raw():
    """Read, concat, and write cboe symbol ref."""
    mkt_list = ['cone', 'opt', 'ctwo', 'exo']
    burl1 = 'https://www.cboe.com/us/options/'
    burl2 = 'market_statistics/symbol_reference/?mkt='
    url_end = '&listed=1&unit=1&closing=1'

    df_list = []
    for mkt in mkt_list:
        url = f"{burl1}{burl2}{mkt}{url_end}"
        get = requests.get(url)

        if get.status_code == 200:
            df_list.append(pd.read_csv(BytesIO(get.content)))
        else:
            help_print_arg(f"Symbol ref request failed for mkt {str(mkt)}")

    df = pd.concat(df_list)
    cols_to_drop = ['Matching Unit', 'Closing Only']
    df.drop(columns=cols_to_drop, inplace=True)
    if df['OSI Symbol'].isna().sum() != 0:
        df.dropna(subset=['OSI Symbol'], inplace=True)
    # %% codecell

    dt = getDate.query('iex_close')
    path_to_write = (Path(baseDir().path,
                     'ref_data/yoptions_ref/cboe_ref_raw', f'_{dt}.parquet'))

    write_to_parquet(df, path_to_write)



# %% codecell





# %% codecell
