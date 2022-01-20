"""Figure out what options data I'm collecting here."""
# %% codecell
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np
from tqdm import tqdm


from multiuse.help_class import getDate, write_to_parquet, baseDir
from multiuse.path_helpers import get_most_recent_fpath
from api import serverAPI

# %% codecell

def treasuries_clean_write():
    """Clean, and store daily treasury data locally."""
    tz = serverAPI('treasuries').df

    tz['time_test'] = pd.to_datetime(tz['time'], unit='ms', errors='coerce')
    tz_mod = tz.dropna(subset=['time_test'])
    tz_mod = tz_mod.drop(columns=['time']).rename(columns={'time_test': 'time'})
    tz = tz[~tz.index.isin(tz_mod.index)].drop(columns=['time_test']).copy()
    tz = pd.concat([tz, tz_mod])
    col_dict = ({'^IRX': 'ThreeM', '^FVX': 'ThreeY',
                 '^TNX': 'FiveY', '^TYX': 'TenY'})
    tz.rename(columns=col_dict, inplace=True)
    tz['time'] = pd.to_datetime(tz['time'])
    tz['date'] = pd.to_datetime(tz['time'].dt.date)
    tz = tz.sort_values(by=['date'])

    tz_daily = tz.groupby(by=['date']).mean()
    path_to_write = Path(baseDir().path, 'economic_data/tz_daily.parquet')
    write_to_parquet(tz_daily, path_to_write)

    return tz_daily

# %% codecell

yoptions_all = serverAPI('yoptions_all').df

shape = yoptions_all.shape[0]
for col in yoptions_all.columns:
    na = yoptions_all[col].isna().sum()
    if (na / shape) > .2:
        print(f"Column {col}: has {na}s  {str(round(na / shape, 2))}")

    try:
        inf = yoptions_all[np.isfinite(yoptions_all[col]) == False].shape[0]
        if inf > 0:
            print(f"{col} {inf}")
    except TypeError:
        pass

# %% codecell

# %% codecell
# ref_data = serverAPI('cboe_symref').df
# min_ref_data = ref_data[['Underlying', 'side', 'expirationDate', 'sym_suf']].copy()
# min_ref_data['contractSymbol'] = min_ref_data.apply(lambda row: f"{row['Underlying']}{row['sym_suf']}", axis=1)
# deriv_all = pd.merge(yoptions_all, min_ref_data, on=['contractSymbol'], how='left')

# %% codecell
path_to_write = Path(baseDir().path, 'derivatives/temp_dump/yderivs_comb.parquet')
# write_to_parquet(deriv_all, path_to_write)
# %% codecell
df_all = pd.read_parquet(path_to_write)
# %% codecell
df_all['sym_suf'].isna().sum()
# For simplicity's sake, lets only work with cleaned data
df_mod = df_all.dropna(subset=['sym_suf'])
path_to_write = Path(baseDir().path, 'derivatives/temp_dump/yderivs_nonan.parquet')
# write_to_parquet(df_mod, path_to_write)
# %% codecell

# %% codecell
path_to_write = Path(baseDir().path, 'derivatives/temp_dump/yderivs_comb.parquet')
df_mod = pd.read_parquet(path_to_write)
# %% codecell

df_mod['expDate'] = df_mod.apply(lambda row: row['contractSymbol'].replace(row['symbol'], '')[:6], axis=1)
df_mod['expDate'] = pd.to_datetime(df_mod['expDate'], format='%y%m%d', errors='coerce')
df_mod['side'] = df_mod.apply(lambda row: row['contractSymbol'].replace(row['symbol'], '')[6], axis=1)

df_mod['vol/oi'] = df_mod['volume'].div(df_mod['openInterest']).round(0)
df_mod['mid'] = (df_mod['ask'].add(df_mod['bid'])).div(2).round(3)
df_mod['premium'] = (df_mod['mid'].mul(df_mod['volume']) * 100).round(0)

cols_to_round = ['lastPrice', 'bid', 'ask', 'percentChange', 'impliedVolatility']
df_mod[cols_to_round] = df_mod[cols_to_round].astype(np.float64)
for col in cols_to_round:
    df_mod[col] = df_mod[col].round(2)

df_mod['vol/oi'] = np.where(np.isfinite(df_mod['vol/oi']), df_mod['vol/oi'], 0)
df_mod['lastDT'] = pd.to_datetime(df_mod['lastTradeDate'].dt.date)
df_mod = df_mod[df_mod['lastDT'] >= '2021-09-30'].copy()
df_mod.drop_duplicates(subset=['contractSymbol', 'lastDT', 'volume'], inplace=True)

df_mod.rename(columns={'date': 'dataDate'}, inplace=True)

path_to_write = Path(baseDir().path, 'dump', 'yoptions_greeks_unfin.parquet')
write_to_parquet(df_mod, path_to_write)
# %% codecell
# write_to_parquet(df_mod, path_to_write)
df_mod = pd.read_parquet(path_to_write)

# I'd like to get the put/call ratio for each expiration
# A summary would be helpful as well
# Put/call volume, open interest, for in the money vs out of the money
# Depending on the expiration date

# Strike weighted avg volume


# %% codecell

# %% codecell

# %% codecell

from workbooks.fib_funcs import read_clean_combined_all

dt = date(2021, 9, 1)
df_all = read_clean_combined_all(dt=dt)

# %% codecell
df_mod.rename(columns={'date': 'dataDate'}, inplace=True)
df_comb = pd.merge(df_mod, df_all[['symbol', 'date', 'fClose']], left_on=['symbol', 'lastDT'], right_on=['symbol', 'date'], how='left')
df_comb = df_comb.dropna(subset=['expDate']).copy()
df_comb['dt_exp'] = getDate.get_bus_day_diff(df_comb, 'lastDT', 'expDate')

# %% codecell
path_to_write = Path(baseDir().path, 'dump', 'yoptions_comb_unfin.parquet')
# write_to_parquet(df_comb, path_to_write)
df_comb = pd.read_parquet(path_to_write)
# %% codecell

df_ocgn = df_comb[df_comb['symbol'] == 'OCGN']



df_ocgn
# %% codecell


df_sub = df_comb[df_comb['symbol'].isin(df_comb['symbol'].unique()[:15])].copy()

df_sub['putIdx'] = np.where(
    (
    (df_sub['symbol'] == df_sub['symbol']) &
    (df_sub['expDate'] == df_sub['expDate']) &
    (df_sub['strike'] == df_sub['strike']) &
    (df_sub['lastDT'] == df_sub['lastDT']) &
    (df_sub['side'] == 'P')
    ),
    df_sub.index, 0
)

df_sub['putIdx'] = np.where(
    (
    (df_sub['symbol'] == df_sub['symbol']) &
    (df_sub['expDate'] == df_sub['expDate']) &
    (df_sub['strike'] == df_sub['strike']) &
    (df_sub['lastDT'] == df_sub['lastDT']) &
    (df_sub['side'] == 'C')
    ),
    df_sub.index, 0
)

df_sub_cols = df_sub[['symbol', 'lastDT', 'strike', 'expDate', 'side', 'volume', 'openInterest']].copy()
df_sub_cols = df_sub_cols.set_index(['symbol', 'lastDT', 'expDate', 'side'])

df_sub_cols.loc['OFIX']
df_sub_cols[['volume']].groupby(df_sub_cols.index, as_index=True).sum()


df_sub_cols
df_sub[['symbol', 'lastDT', 'strike', 'expDate', 'side', 'volume']].groupby(by=['symbol', 'lastDT', 'strike', 'expDate', 'side']).sum()


df_sub['putIdx']
# df_sub[df_sub['side'] == 'P']['volume'].div(df_sub[df_sub['side'] == 'C']['volume'])

df_sub[df_sub['contractSymbol'] == 'OLN211119P00055000']

(df_sub[(df_sub['symbol'] == df_sub['symbol']) &
(df_sub['expDate'] == df_sub['expDate']) &
(df_sub['strike'] == df_sub['strike']) &
(df_sub['lastDT'] == df_sub['lastDT'])])


df_sub['p/c'].value_counts()


# %% codecell
tz_daily = treasuries_clean_write()
tz_daily = tz_daily.reset_index()
df_comb = pd.merge(df_comb, tz_daily[['date', 'ThreeM']], on=['date'], how='left')

path_to_write = Path(baseDir().path, 'dump', 'yoptions_greeks_unfin.parquet')
write_to_parquet(df_comb, path_to_write)
# %% codecell


# %% codecell

# Get IV rank for each symbol

df_sub = df_mod[df_mod['symbol'].isin(df_mod['symbol'].unique()[:10])].copy()
df_sub['impliedVolatility']

df_sub = df_sub.set_index('contractSymbol')
df_sub_imp = df_sub[['date', 'impliedVolatility']].copy()

idx_unique = df_sub_imp.index.unique()
for idx in idx_unique:
    print(df_sub_imp.loc[idx])
    break

idx_unique

df_sub_imp['impRank'] = df_sub_imp['impliedVolatility'].rank(pct=True)

df_sub_imp['impRank'].sort_values(ascending=False)
df_sub_imp


# %% codecell


# %% codecell



# %% codecell
serverAPI('redo', val='combine_eod_to_years')

serverAPI('redo', val='combine_daily_stock_eod')


# %% codecell




# %% codecell

# %% codecell


# %% codecell
