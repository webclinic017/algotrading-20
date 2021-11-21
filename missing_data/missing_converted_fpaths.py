"""Get fpaths that weren't converted."""
# %% codecell
from tqdm import tqdm
import pandas as pd

try:
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from api import serverAPI

# %% codecell


def fpath_not_converted():
    """Get fpaths that weren't converted to parquet files."""
    fpath_list = serverAPI('fpath_list').df
    fpath_list.rename(columns={0: 'fpath'}, inplace=True)

    not_converted = []
    sub_str = '_all_combined_not_converted.gz'
    for fpath in tqdm(fpath_list['fpath']):
        if sub_str in (fpath):
            not_converted.append(fpath)

    return not_converted

# %% codecell
