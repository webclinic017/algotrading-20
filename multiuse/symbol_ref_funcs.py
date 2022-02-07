"""Symbol reference data functions."""
# %% codecell
from pathlib import Path
import os
from dotenv import load_dotenv

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg
    from api import serverAPI
# %% codecell


def remove_funds_spacs(query='symbol not in @exclude_list'):
    """Remove funds and other non-common stock symbols."""
    all_syms = serverAPI('all_symbols').df
    cond_str = all_syms['name'].str.lower().str
    # Exclude dict
    edict = {}
    edict['spacs'] = all_syms[cond_str.contains('acquisition')]
    edict['funds'] = all_syms[cond_str.contains('fund')]
    edict['etfs'] = all_syms[all_syms['type'] == 'et']

    other_list = ['ps', 'rt', 'struct', 'ut', 'wt']
    edict['others'] = all_syms[all_syms['type'].isin(other_list)]

    edict['inv_corp'] = all_syms[cond_str.contains('investment corp')]
    edict['capital'] = all_syms[cond_str.contains('capital')]
    edict['trusts'] = all_syms[(cond_str.contains('trust'))]

    # Exclude certain words
    words_to_exclude = (['reit', 'municipal', 'income', 'merger',
                         'investors', ' spac ', ' i ', ' ii ',
                         ' iv ', ' v ', ' vi '])
    for word in words_to_exclude:
        edict[word] = all_syms[cond_str.contains(word)]

    exclude_list = pd.concat([val['symbol'] for val in edict.values()])
    result = all_syms.query(query).reset_index(drop=True)

    return result


def get_all_symbol_ref():
    """Get all common and OTC symbols."""
    load_dotenv()
    env = os.environ.get("env")

    df_all = None

    if env == "production":
        bpath = Path(baseDir().path, 'tickers', 'symbol_list')

        com_syms_path = bpath.joinpath('all_symbols.parquet')
        otc_syms_path = bpath.joinpath('otc_syms.parquet')
        com_df = pd.read_parquet(com_syms_path)
        otc_df = pd.read_parquet(otc_syms_path)
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

def get_symbol_stats():
    """Get stats for all symbols."""
    try:
        from scripts.dev.api import serverAPI
    except ModuleNotFoundError:
        from api import serverAPI

    og_stats = serverAPI('stats_combined').df
    stats = og_stats[og_stats['date'] != 'nan']
    stats_max = stats[stats['date'] == stats['date'].max()].reset_index(drop=True).copy()

    # All non-otc symbols reference data
    all_syms = serverAPI('all_symbols').df
    all_syms.drop(columns=['date'], inplace=True)
    df_stats = pd.merge(stats_max, all_syms, left_on=['companyName'], right_on=['name'], how='inner')

    # Company meta information
    meta_sapi = serverAPI('company_meta')
    meta_df = meta_sapi.df
    meta_df['spac'] = np.where(meta_df['companyName'].str.contains('Acquisition'), 1, 0)
    meta_df['fund'] = np.where(meta_df['companyName'].str.contains('Fund'), 1, 0)

    non_fund_spac = (meta_df[(meta_df['spac'] != 1) & (meta_df['fund'] != 1)]
                     .drop_duplicates(subset=['symbol']))
    cols_to_keep = ['symbol', 'industry', 'sector', 'tags']
    non_fund_spac = non_fund_spac[cols_to_keep]

    df_stats = pd.merge(non_fund_spac, df_stats, on=['symbol'])

    return df_stats

# %% codecell
