"""Max/min values for fib predictions."""
# %% codecell

from pathlib import Path
from tqdm import tqdm

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
    from scripts.dev.multiuse.pd_funcs import mask, chained_isin
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet
    from multiuse.pd_funcs import mask, chained_isin

# %% codecell
pd.DataFrame.mask = mask
pd.DataFrame.chained_isin = chained_isin
pd.set_option('display.max_columns', 50)

# %% codecell
bpath = Path(baseDir().path, 'ml_data', 'fib_analysis')
fib_df_path = bpath.joinpath('fib_df_temp.parquet')
df_all_path = bpath.joinpath('df_all_temp.parquet')


fib_df = pd.read_parquet(fib_df_path)
df_all = pd.read_parquet(df_all_path)

# %% codecell

ret_ext_cols = [col for col in fib_df.columns.tolist() if '.' in col]
cols_to_round = ret_ext_cols + ['start', 'high', 'rsi', 'range', 'ext_end', 'fibPercRange']

fib_df[cols_to_round] = fib_df[cols_to_round].astype(np.float64).round(2)
fib_df.rename(columns={'range': 'fibRange', 'date': 'maxDate'}, inplace=True, errors='ignore')
fib_df.drop(columns=['marketcap'], inplace=True)

cols_to_drop = ['sma50', 'sma200']
all_cols_to_round = ['fClose', 'prev_close', 'rsi', 'vol_avg_2m', 'sma_50', 'sma_200']
df_all.drop(columns=cols_to_drop, inplace=True)
df_all.rename(columns={'sharesOutstanding': 'sharesOut'}, inplace=True)
df_all[all_cols_to_round] = df_all[all_cols_to_round].astype(np.float64).round(2)

df_m = pd.merge(df_all, fib_df, on=['symbol'])

merge_path = bpath.joinpath('df_m_temp.parquet')
write_to_parquet(df_m, merge_path)

# %% codecell

bpath = Path(baseDir().path, 'ml_data', 'fib_analysis')
merge_path = bpath.joinpath('df_m_temp.parquet')
df_m = pd.read_parquet(merge_path)
ret_ext_cols = [col for col in df_m.columns.tolist() if '.' in col]

# %% codecell
# Days to reach each of these levels from max_date
sym_list = df_m['symbol'].unique().tolist()
df_list = []
n = 0
cutoff = 7000

for sym in tqdm(sym_list):
    df_sym = df_m.mask('symbol', sym)
    df_sym_p = df_sym.mask('date', df_sym['maxDate'].max(), equals=False, greater=True)

    max_row = df_sym.mask('date', df_sym['maxDate'].max())
    max_idx = max_row.index[0]

    fib_days = {}
    na_row = {'dBefX': np.NaN, 'dPastPerc': np.NaN, 'dBefCross': np.NaN}
    for col in ret_ext_cols:
        fib_days[col] = {}
        try:
            if 'ret' in col:
                bret = df_sym_p.mask('fLow', df_sym_p[col].iloc[0], equals=False, lesser=True)
                bretCross = df_sym_p.mask('fLow', df_sym_p[col].iloc[0], equals=False, greater=True)
                fib_days[col]['dBefX'] = bret.head(1)['date'].index[0] - max_idx
                fib_days[col]['dPastPerc'] = np.round((bret.shape[0] / df_sym_p.shape[0]), 2)
                try:
                    fib_days[col]['dBefCross'] = bretCross.head(1)['date'].index[0] - max_idx
                except IndexError:
                    fib_days[col]['dBefCross'] = np.NaN
            elif 'ext' in col:
                bext = df_sym_p.mask('fHigh', df_sym_p[col].iloc[0], equals=False, greater=True)
                bextCross = df_sym_p.mask('fHigh', df_sym_p[col].iloc[0], equals=False, lesser=True)
                fib_days[col]['dBefX'] = bext.head(1)['date'].index[0] - max_idx
                fib_days[col]['dPastPerc'] = np.round((bext.shape[0] / df_sym_p.shape[0]), 2)
                try:
                    fib_days[col]['dBefCross'] = bextCross.head(1)['date'].index[0] - max_idx
                except IndexError:
                    fib_days[col]['dBefCross'] = np.NaN
        except IndexError:
            fib_days[col] = na_row

    df = pd.DataFrame.from_dict(fib_days.values()).copy()
    df.insert(0, 'symbol', sym)
    df.insert(1, 'fibVal', ret_ext_cols)

    df_list.append(df)

    n += 1
    if n > cutoff:
        break
        # print(col)

df_days_all = pd.concat(df_list).reset_index(drop=True)

# %% codecell

# I could make a range condition based soley on if the fHigh was higher than previous.
# And one for the fLow being lower than the next

fib_df.mask('symbol', 'SAGE')

beta_path = Path(baseDir().path, 'studies/beta')
from multiuse.path_helpers import get_most_recent_fpath
path = get_most_recent_fpath(beta_path)
df_beta = pd.read_parquet(path)

df_days = pd.merge(df_days_all, fib_df[['symbol', 'date_range']], on='symbol')
df_days = pd.merge(df_days, df_beta, on=['symbol'], how='left')

df_m_ma = df_m[['symbol', 'up50', 'up200']]
df_m_ma_group = df_m_ma.groupby(by=['symbol'], as_index=False).sum()
df_days = pd.merge(df_days, df_m_ma_group, on=['symbol'])
df_days = df_days[df_days['beta'].abs() > 1.5].copy()
# %% codecell
df_days

# %% codecell
fib_df.mask('symbol', 'BIGC')

# How many times does the fLow cross before all 4 are below the line?
# What is volume doing? After the initial high, does the avg go down?
# I'd also like to see the correlation between cumPerc and stocktwits mentions

# Can also do corp eval - sales ratios, pe (if any).
# It'd be helpful to parse all the SEC statements to find out
# share offerings otherwise I'm kinda flying blind here.

# Look for gaps too

df_days.mask('fibVal', 'ext_0.999').mask('dPastPerc', .25, equals=False, lesser=True).dropna(subset=['dPastPerc']).sort_values(by=['dPastPerc'], ascending=True).head(25)


df_days_corr = df_days.groupby(by=['fibVal']).corr()
df_days_corr.head(50)

# %% codecell





# %% codecell




# %% codecell
