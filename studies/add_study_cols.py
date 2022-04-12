"""Add study columns to dataframe."""
# %% codecell
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm
import talib

try:
    from scripts.dev.multiuse.pd_funcs import mask, perc_change
    from scripts.dev.multiuse.help_class import getDate
except ModuleNotFoundError:
    from multiuse.pd_funcs import mask, perc_change
    from multiuse.help_class import getDate


pd.DataFrame.mask = mask
pd.DataFrame.perc_change = perc_change
# %% codecell


# %% codecell


def first_cleanup_basic_cols(df_all, **kwargs):
    """Cleanup data then add fRange and other cols."""
    dt = kwargs.get('dt', getDate.query('iex_eod'))
    if type(dt) is bool:
        dt = getDate.query('iex_eod')

    if df_all['date'].dtype == 'object':
        df_all['date'] = pd.to_datetime(df_all['date'])
    # Drop duplicates on symbol and date
    df_all = (df_all.drop_duplicates(subset=['symbol', 'date'])
                    .reset_index(drop=True)
                    .copy())

    # Get rid of all symbols that only have 1 day of data
    df_vc = df_all['symbol'].value_counts()
    df_vc_1 = df_vc[df_vc == 1].index.tolist()
    df_all = (df_all[~df_all['symbol'].isin(df_vc_1)]
              .reset_index(drop=True)
              .copy())

    # Sort by symbol, date ascending
    df_all = df_all.sort_values(by=['symbol', 'date'], ascending=True)

    # Exclude all dates from before this year
    df_all = (df_all[df_all['date'] >= str(dt.year)]
              .dropna(subset=['fVolume'])
              .reset_index(drop=True)
              .copy())

    df_all['fRange'] = (df_all['fHigh'] - df_all['fLow'])
    df_all['vol/mil'] = (df_all['fVolume'].div(1000000))
    df_all['prev_close'] = df_all['fClose'].shift(periods=1, axis=0)
    df_all['prev_symbol'] = df_all['symbol'].shift(periods=1, axis=0)

    return df_all


def add_fChangeP_col(df_all):
    """Add percent change under the column fChangeP."""
    df_mod = df_all[['symbol', 'date', 'fClose']].copy()
    try:
        df_mod_1 = (df_mod.pivot(index=['symbol'],
                                 columns=['date'],
                                 values=['fClose']))
        df_mod_2 = (df_mod_1.pct_change(axis='columns',
                                        fill_method='bfill',
                                        limit=1))
        df_mod_3 = (df_mod_2.stack()
                            .reset_index()
                            .rename(columns={'fClose': 'fChangeP'}))
        df_all = (pd.merge(df_all, df_mod_3,
                           how='left',
                           on=['date', 'symbol']))
    except ValueError:
        df_mod['fCloseS1'] = df_mod['fClose'].shift(1)
        df_mod['prev_symbol'] = df_mod['symbol'].shift(1)

        df_mod = (df_mod.join(
                   df_mod.perc_change('fCloseS1', 'fClose')
                         .rename('fChangeP'))
                  .copy())

        sym_not_prev_idx = (df_mod[df_mod['symbol'] !=
                            df_mod['prev_symbol']].index)
        df_mod.loc[sym_not_prev_idx, 'fChangeP'] = 0

        cols_to_drop = ['date', 'symbol', 'fClose', 'fCloseS1', 'prev_symbol']
        df_all = df_all.join(df_mod.drop(columns=cols_to_drop)).copy()

    return df_all


def add_gap_col(df_all):
    """Add up/down gap column to df_all."""
    not_the_same = df_all[df_all['symbol'] != df_all['prev_symbol']]
    df_all.loc[not_the_same.index, 'prev_close'] = np.NaN
    # df_all.drop(columns='prev_symbol', inplace=True)
    gap_cond_up = (df_all['prev_close'] * 1.025)
    gap_cond_down = (df_all['prev_close'] * .975)

    df_all['gap'] = (np.where(~df_all['fOpen'].between(
                     gap_cond_down, gap_cond_up), 1, 0))

    gap_up = ((df_all['fOpen'] > gap_cond_up))
    gap_down = ((df_all['fOpen'] < gap_cond_down))
    gap_rows = df_all[gap_up | gap_down]

    df_all.loc[gap_rows.index, 'gap'] = (gap_rows[['fOpen', 'prev_close']]
                                         .pct_change(axis='columns',
                                                     periods=-1)
                                         ['fOpen'].values.round(3))
    cols_to_round = ['fOpen', 'fLow', 'fClose', 'fHigh']
    df_all.dropna(subset=cols_to_round, inplace=True)
    df_all.loc[:, cols_to_round] = df_all[cols_to_round].round(3)

    return df_all


def calc_rsi(df):
    """Calculate and add RSI, overbought, oversold."""
    rsi_vals = []
    df_all_sym = df[['symbol', 'fClose']].set_index('symbol').copy()
    df_all_sym['fClose'] = df_all_sym['fClose'].astype(np.float64)
    sym_list = (sorted(df_all_sym.index.get_level_values('symbol')
                       .unique().dropna().tolist()))
    n = 0
    for symbol in tqdm(sym_list):
        try:
            prices = df_all_sym.loc[symbol]['fClose'].to_numpy()
            # rsi_vals = np.append(rsi_vals, talib.RSI(prices))
            rsi_vals.append(talib.RSI(prices))
        except AttributeError:  # For symbols that don't exist
            n += 1
            print(symbol)
            rsi_vals.append(np.zeros(1))
            # If there's a symbol error, add zeroes of equiv length

            if n > 100:  # Assume something else is wrong
                break
                return df

    # rsi_vals = np.array(rsi_vals)
    df['rsi'] = np.concatenate(rsi_vals)
    df['rsi_ob'] = np.where(df['rsi'] > 70, 1, 0)
    df['rsi_os'] = np.where(df['rsi'] < 30, 1, 0)

    return df


def make_moving_averages(df_all):
    """Make moving averages for dataframe."""
    # Create new dataframe and set the index to symbol
    df_all_sym = df_all.set_index('symbol')
    df_all['sma_50'] = (df_all_sym['fClose'].rolling(min_periods=50,
                                                     window=50)
                                            .mean().to_numpy())
    df_all['sma_200'] = (df_all_sym['fClose'].rolling(min_periods=200,
                                                      window=200)
                                             .mean().to_numpy())

    df_all['prev_symbol'] = df_all['symbol'].shift(periods=1, axis=0)
    cols_to_cat = ['symbol', 'prev_symbol']
    df_all[cols_to_cat] = df_all[cols_to_cat].astype("category")
    # Convert back to category ^
    df_all['up50'] = (df_all.mask('symbol', df_all['prev_symbol'])
                      ['sma_50'].diff())
    df_all['up200'] = (df_all.mask('symbol', df_all['prev_symbol'])
                       ['sma_200'].diff())
    df_all['up50'] = np.where(df_all['up50'] > 0, 1, -1)
    df_all['up200'] = np.where(df_all['up200'] > 0, 1, -1)

    return df_all


def add_fHighMax_col(df_all):
    """Add fHighMax column."""
    max_val = 0
    max_test = []

    for index, row in tqdm(df_all[['symbol', 'fHigh', 'prev_symbol']].iterrows()):
        if row['symbol'] != row['prev_symbol']:
            max_val = 0
        if row['fHigh'] > max_val:
            max_val = row['fHigh']
            max_test.append(row['fHigh'])
        else:
            max_test.append(np.NaN)

    df_all['fHighMax'] = max_test

    return df_all


# %% codecell


def std_ann_deviation(df):
    """Calculate std dev and annualized std dev."""

    df['logfCP'] = np.where(
        df['symbol'] == df['prev_symbol'],
        np.log(df['fChangeP']),
        np.NaN
    )

    df_sym = df.set_index('symbol')
    syms = df_sym.index.unique()

    std_dev = []
    for sym in tqdm(syms):
        std_dev.append(np.std(df_sym.loc[sym]['fChangeP']))

    syms_std_dev = (pd.Series({sym: std for sym, std in zip(syms, std_dev)},
                              name='s_std'))
    df_sym['stdDev'] = df_sym.index.map(syms_std_dev).astype(np.float64)
    df_sym['annStdDev'] = df_sym['stdDev'] * 252 ** 0.5

    df_sym = df_sym.drop(columns=['logfCP']).copy()
    df = df_sym.reset_index().copy()

    return df

# %% codecell
