"""Symbol reference data functions."""
# %% codecell
from pathlib import Path
import os
from dotenv import load_dotenv

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg

# %% codecell


def get_all_symbol_ref():
    """Get all common and OTC symbols."""
    load_dotenv()
    env = os.environ.get("env")

    df_all = None

    if env == "production":
        base_path = Path(baseDir().path, 'tickers')

        com_syms_path = Path(base_path, 'all_symbols.gz')
        otc_syms_path = Path(base_path, 'otc_syms.gz')
        com_df = pd.read_json(com_syms_path, compression='gzip')
        otc_df = pd.read_json(otc_syms_path, compression='gzip')
        otc_df.dropna(subset=['cik'], inplace=True)
        otc_df['cik'] = (otc_df['cik'].astype('int64').astype('str').str
                                      .zfill(10).astype('category')
                                      .reset_index(drop=True))
        df_all = pd.concat([com_df, otc_df]).reset_index(drop=True)
    else:
        try:
            from api import serverAPI
            com_syms = serverAPI('all_symbols').df
            otc_syms = serverAPI('otc_syms').df
            df_all = pd.concat([com_syms, otc_syms]).reset_index(drop=True)
        except ModuleNotFoundError:
            help_print_arg('Tried import server api in get_all_symbols func')

    return df_all


# %% codecell
