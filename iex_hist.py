"""IEX Historical with TA Lib."""
# %% codecell
from pathlib import Path
import os
from tqdm import tqdm

import pandas as pd
import numpy as np
import talib
from talib import abstract
from multiuse.help_class import baseDir, dataTypes, getDate

# %% codecell
# Display max 50 columns
pd.set_option('display.max_columns', 100)
# Display maximum rows
pd.set_option('display.max_rows', 100)

# %% codecell
path = Path(baseDir().path,  'historical', '2021')

price_cols = ['fOpen', 'fHigh', 'fLow', 'fClose']
cols_to_read = ['fVolume'] + price_cols

df_list = [pd.read_parquet(fpath) for fpath in list(path.glob('**/*.parquet')) if os.path.getsize(fpath) > 0]
df = pd.concat(df_list)
# %% codecell
df = df.set_index(['symbol', 'date'])
df_sub = df[cols_to_read].copy()

combined_fpath = Path(baseDir().path, 'historical', 'combined', 'sub.parquet')
combined_fpath.resolve()

df_sub = dataTypes(df_sub).df

df_sub.to_parquet(combined_fpath)

# %% codecell
combined_fpath = Path(baseDir().path, 'historical', 'combined', 'sub.parquet')
df = pd.read_parquet(combined_fpath)

price_cols = ['fOpen', 'fHigh', 'fLow', 'fClose']
cols_to_read = ['fVolume'] + price_cols
df[cols_to_read] = df[cols_to_read].astype(np.float64)

df.info()

# %% codecell
symbol_list = (df.index
                 .get_level_values('symbol')
                 .unique()
                 .to_numpy())


# df_aapl = df.loc['AAPL']
# %% codecell

ta_funcs = talib.get_functions()
ta_cdls = [ta for ta in ta_funcs if 'CDL' in ta]
ta_cdls_dict = {cdl: abstract.Function(cdl) for cdl in ta_cdls}
ta_funcs_dict = {func: abstract.Function(func) for func in ta_funcs}

def create_input_arrays(df):
    """Return input arrays to use for talib."""
    input_arrays = {
        'open': np.array(df['fOpen'].values),
        'high': np.array(df['fHigh'].values),
        'low': np.array(df['fLow'].values),
        'close': np.array(df['fClose'].values)
        # 'volume': df_aapl['fVolume'].values
    }

    return input_arrays
# %% codecell
# %% codecell
for cdl in ta_cdls:
    print(cdl)
    # df_aapl[cdl] = ta_cdls_dict[cdl](open=inputs[0], high=inputs[1], low=inputs[2], close=inputs[3]))
df[cols_to_read] = df[cols_to_read].astype(np.float64)


# %% codecell

df_mod = df.copy()

df_mod = df_mod[df_mod.index.get_level_values('date') > '2020']

symbol_list = (df_mod.index
                     .get_level_values('symbol')
                     .unique()
                     .to_numpy())
df_mod.info()
# df_mod[ta_cdls] = np.zeros(df_mod.shape[0], dtype=np.int8)

# %% codecell
for cdl in ta_cdls:
    df_mod[cdl] = 0

# %% codecell
for sym in tqdm(symbol_list):
    inputs = create_input_arrays(df_mod.loc[sym])

    for cdl in ta_cdls:
        df_mod.loc[sym, cdl] = ta_cdls_dict[cdl](inputs)

# %% codecell
df_mod[ta_cdls].value_counts()

cdl_mod_fpath = Path(baseDir().path, 'historical', 'combined', 'cdl_mod_sub.parquet')
df_mod[ta_cdls] = df_mod[ta_cdls].astype(np.int8)

df_mod.to_parquet(cdl_mod_fpath)
df_mod.info()

# df_mod.loc[sym][cdl]._is_view
# df_mod.loc[sym][cdl]._is_copy
# %% codecell

df_mod.loc['SHEN']


# %% codecell
ta_funcs_dict['EMA']

funcs_to_add = ['VAR', 'OBV', 'RSI', 'MACD']
extras = ['MACDSIGNAL', 'MACDHIST']

for func in funcs_to_add + extras:
    df_mod[func] = 0


funcs_to_add = ['EMA_10', 'EMA_20', 'EMA_50', 'EMA_200']
for func in funcs_to_add:
    if 'EMA_' in func:
        split = func.split('_')[1]
        inputs = df_mod.loc[sym]['fClose'].values
        output = ta_funcs_dict['EMA'](inputs, timeperiod=int(split))
        df_mod.loc[sym, func] = output

for func in funcs_to_add:
    df_mod[func] = 0

funcs_to_add
# %% codecell
for sym in tqdm(symbol_list):
    inputs = {'close': df_mod.loc[sym]['fClose'].values,
              'volume': df_mod.loc[sym]['fVolume'].values}
    for func in funcs_to_add:
        if func == 'MACD':
            output = ta_funcs_dict[func](inputs)
            df_mod.loc[sym, 'MACD'] = output[0]
            df_mod.loc[sym, 'MACDSIGNAL'] = output[1]
            df_mod.loc[sym, 'MACDHIST'] = output[2]
        if 'EMA_' in func:
            split = func.split('_')[1]
            inputs = df_mod.loc[sym]['fClose'].values
            output = ta_funcs_dict['EMA'](inputs, timeperiod=int(split))
            df_mod.loc[sym, func] = output
        else:
            df_mod.loc[sym, func] = ta_funcs_dict[func](inputs)


# %% codecell

df_mod.info()
macd_cols = [col for col in df_mod.columns if 'MACD' in col]
ta_cdls
df_mod.loc['AAPL', 'RSI'].tail(50)


func_groups = talib.get_function_groups()
func_groups.keys()
func_groups['Pattern Recognition']

# %% codecell
for cdl in ta_cdls:
    (print(
        df_aapl[(df_aapl[cdl] != 0) & (df_aapl.index > '2020')][cdl]
    ))
# %% codecell






























# %% codecell
