"""Intraday summary for cboe options."""
# %% codecell
from pathlib import Path
from datetime import timedelta

import pandas as pd
import numpy as np
from tqdm import tqdm

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet, help_print_arg, round_cols
    from scripts.dev.multiuse.pd_funcs import mask
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet, help_print_arg, round_cols
    from multiuse.pd_funcs import mask
    from api import serverAPI


# %% codecell
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 50)

pd.DataFrame.mask = mask

import importlib
import sys

from intraday.options_cboe_funcs import IntradayOptionsCBOEHelpers
importlib.reload(sys.modules['intraday.options_cboe_funcs'])
from intraday.options_cboe_funcs import IntradayOptionsCBOEHelpers

from data_collect.bz_data import WebScrapeBzRates
importlib.reload(sys.modules['data_collect.bz_data'])
from data_collect.bz_data import WebScrapeBzRates

from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

# %% codecell

iocboe = IntradayOptionsCBOEHelpers()

# %% codecell


class IntradayOptionsSummary():
    """Get and summarize derivatives intraday data."""


# %% codecell

CombineCboeIntraday(verbose=False)

# %% codecell
# serverAPI('redo', val='CombineCboeIntraday')




# %% codecell

path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals.parquet')
df_test = pd.read_parquet(path)

# %% codecell

df_test.mask('symbol', 'PYPL')


# %% codecell

cboe_intra = serverAPI('cboe_intraday_intra').df

cboe_intra['date'] = pd.to_datetime(cboe_intra['date'])
cboe_intra['Expiration'] = pd.to_datetime(cboe_intra['Expiration'])

cboe_intra['cs_pre'] = cboe_intra['Symbol'].astype('str') + cboe_intra['Expiration'].dt.strftime('%y%m%d') + cboe_intra['Call/Put'].astype('str')

cboe_intra['cs_suf'] = cboe_intra['Strike Price'].astype('str') + '00'
cboe_intra['cs_suf'] = cboe_intra['cs_suf'].str.zfill(9).str.replace('.', '', regex=False)
cboe_intra['contractSymbol'] = cboe_intra['cs_pre'] + cboe_intra['cs_suf']

cboe_vix = (cboe_intra[cboe_intra['Symbol'] == 'VIX']
            .drop(columns=['cs_pre', 'cs_suf'])
            .copy())


# %% codecell

other_cols = ['contractSymbol', 'time']
cols_to_dupe = ['contractSymbol', 'Volume', 'Last Price']

cboe_vix[other_cols].value_counts()
cboe_vix[cols_to_dupe].value_counts()
cboe_vix.drop_duplicates(subset=cols_to_dupe)

cboe_vix[cboe_vix['contractSymbol'] == 'VIX220131C00070000']

cboe_vix
# %% codecell


# %% codecell

# %% codecell


# %% codecell

# %% codecell


def _create_summary_df(column, df, cutoff=False, only_new_col=False):
    """Create summary dataframe from c/p ratios."""
    cols_to_group = ['symbol', 'call/put']
    cols_to_inc = cols_to_group + [column]

    col_dict = ({'premium': 'pRem/cRem', 'volume': 'P/C',
                 'vol/oi': 'voiP/voiC'})
    ncn = col_dict[column]

    df_mod = df[cols_to_inc].groupby(by=cols_to_group).sum().reset_index()
    df_mod[column] = np.where(df_mod[column] == 0, 1, df_mod[column])

    # Pivot call and puts to their own unqiue columns
    df_piv = df_mod.pivot(index='symbol', columns='call/put', values=column)
    df_piv[ncn] = df_piv['P'].div(df_piv['C'])

    if cutoff:
        cut1 = (df_piv['C'] > cutoff)
        cut2 = (df_piv['P'] > cutoff)
        df_piv = df_piv[(cut1 | cut2)]

    if only_new_col:
        df_piv.drop(columns=['C', 'P'], inplace=True)
    else:
        col_dict = {'C': f"{column}C", 'P': f"{column}P"}
        df_piv.rename(columns=col_dict, inplace=True)

    return df_piv

# %% codecell
int_df = iocboe.df

sum_df_list = []
sum_df_list.append(_create_summary_df('premium', int_df, cutoff=False, only_new_col=False))

sum_df_list.append(_create_summary_df('volume', int_df, cutoff=False, only_new_col=True))

sum_df_list.append(_create_summary_df('vol/oi', int_df, only_new_col=True))

ct_all = pd.concat(sum_df_list, axis=1)
# %% codecell
rank_all = ct_all.copy()
for col in rank_all.columns:
    rank_all[col] = rank_all[col].rank(pct=True)

cols = rank_all.columns
bcols = ['pRem/cRem', 'P/C', 'voiP/voiC']

bull_mod = rank_all[rank_all[bcols] < .10].dropna(subset=bcols)
bear_mod = rank_all[rank_all[bcols] > .90].dropna(subset=bcols)

bull_mod = rank_all[rank_all.index.isin(bull_mod.index)].copy()
bear_mod = rank_all[rank_all.index.isin(bear_mod.index)].copy()

# %% codecell


# %% codecell



# %% codecell


meta = _call_stock_meta_info()
stats = _get_stats_info()
df_info = pd.merge(meta, stats, left_index=True, right_index=True)

ct_comb = pd.merge(ct_all, df_info, left_index=True, right_index=True)

prem_cols = ['premiumC', 'premiumP']
ct_comb[prem_cols] = ct_comb[prem_cols] * 100

bull_info = round_cols(ct_comb[ct_comb.index.isin(bull_mod.index)].copy())
bear_info = round_cols(ct_comb[ct_comb.index.isin(bear_mod.index)].copy())

# %% codecell




# Could keep the last month worth of yoptions
# data instead of having to clean it every time

# I'd like to rank all the stocks now:
# put/call premiums - both the highest and the lowest
# put/call volume - both the highest and the lowest
# vol/oi - both the highest and the lowest


ytest = yoptions.sort_values(by=['lastTradeDate']).reset_index(drop=True)
vc = intra_df['contractSymbol'].value_counts()


# %% codecell


fpath = '/Users/eddyt/Algo/data/ref_data/peer_list/_peers.parquet'
df_peers = pd.read_parquet(fpath)


df_peers.mask('key', 'OCGN').sort_values(by=['corr'])
df_peers


# %% codecell

# %% codecell


# %% codecell
