"""Functions for fibonacci sequence analysis."""
# %% codecell
import os
import sys
from pathlib import Path
from io import BytesIO
import importlib
from tqdm import tqdm
import math

import datetime

import pandas as pd
import numpy as np
import yfinance as yf
import talib

try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet, check_nan
    from scripts.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.multiuse.pd_funcs import mask, chained_isin
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet, check_nan
    from multiuse.create_file_struct import makedirs_with_permissions
    from multiuse.path_helpers import get_most_recent_fpath
    from multiuse.pd_funcs import mask, chained_isin
    from api import serverAPI

# %% codecell
pd.DataFrame.mask = mask
pd.DataFrame.chained_isin = chained_isin

# %% codecell

def read_clean_combined_all(local=False, dt=None):
    """Read, clean, and add columns to StockEOD combined all."""
    df_all = None

    if local:
        bpath = Path(baseDir().path, 'StockEOD/combined_all')
        fpath = get_most_recent_fpath(bpath)
        cols_to_read = ['date', 'symbol', 'fOpen', 'fHigh', 'fLow', 'fClose', 'fVolume']
        df_all = pd.read_parquet(fpath, columns=cols_to_read)
        if df_all['date'].dtype == 'object':
            df_all['date'] = pd.to_datetime(df_all['date'])
        df_all.drop_duplicates(subset=['symbol', 'date'], inplace=True)
    else:
        all_syms = serverAPI('all_symbols').df
        all_cs = all_syms[all_syms['type'].isin(['cs', 'ad'])]
        all_cs_syms = all_cs['symbol'].unique().tolist()

        cols_to_read = ['date', 'symbol', 'fOpen', 'fHigh', 'fLow', 'fClose', 'fVolume']
        df_all = serverAPI('stock_close_cb_all').df
        df_all = df_all[cols_to_read]
        df_all = df_all[df_all['symbol'].isin(all_cs_syms)].copy()
        df_all['date'] = pd.to_datetime(df_all['date'])
        df_all.drop_duplicates(subset=['symbol', 'date'], inplace=True)
        df_all.reset_index(drop=True, inplace=True)

    if not dt:
        dt = getDate.query('iex_eod')
    # Exclude all dates from before this year
    df_all = df_all[df_all['date'] >= str(dt.year)].copy()

    df_all['fRange'] = (df_all['fHigh'] - df_all['fLow']).round(2)

    # Add percent change under the column fChangeP
    df_mod = df_all[['symbol', 'date', 'fClose']].copy()
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
    # Merge with df_all and resume

    df_all.dropna(subset=['fVolume'], inplace=True)
    df_all['vol/mil'] = (df_all['fVolume'].div(1000000)).round(2)

    # Add gap column
    df_all = add_gap_col(df_all)

    # Add range of gap
    df_all['gRange'] = (np.where(df_all['prev_close'] < df_all['fLow'],
                                 df_all['fHigh'] - df_all['prev_close'],
                                 df_all['fHigh'] - df_all['fLow']))

    df_all['cumPerc'] = np.where(
        df_all['symbol'] == df_all['prev_symbol'],
        df_all['fChangeP'] + df_all['fChangeP'].shift(1),
        np.NaN)

    df_all['vol_avg_2m'] = np.where(
        df_all['symbol'] == df_all['prev_symbol'],
        df_all['fVolume'].rolling(60).mean(),
        np.NaN)

    # Add cumulative sum of last 5 fChangeP rows
    df_all['fCP5'] = (np.where(df_all['symbol'] == df_all['prev_symbol'],
                      df_all['fChangeP'].rolling(min_periods=1, window=5).sum(),
                      0))

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
    df_all = calc_rsi(df_all)
    df_all = make_moving_averages(df_all)

    # fChange P already takes the previous close value into account
    # df_all['fChangeP'] = ((df_all['fClose'] - df_all['prev_close']) / df_all['prev_close']).round(3)
    # .astype('uint32')
    df_all = df_all.sort_values(by=['symbol', 'date'], ascending=True)

    float_32s = df_all.dtypes[df_all.dtypes == np.float32].index
    for col in float_32s:
        df_all[col] = df_all[col].astype(np.float64).round(3)

    df_all = dataTypes(df_all, parquet=True).df.copy()

    return df_all


def add_gap_col(df_all):
    """Add up/down gap column to df_all."""
    df_all['prev_close'] = df_all['fClose'].shift(periods=1, axis=0)
    df_all['prev_symbol'] = df_all['symbol'].shift(periods=1, axis=0)
    not_the_same = df_all[df_all['symbol'] != df_all['prev_symbol']]
    df_all.loc[not_the_same.index, 'prev_close'] = np.NaN
    # df_all.drop(columns='prev_symbol', inplace=True)
    gap_cond_up = (df_all['prev_close'] * 1.025)
    gap_cond_down = (df_all['prev_close'] * .975)

    df_all['gap'] = np.where(~df_all['fOpen'].between(gap_cond_down, gap_cond_up), 1, 0)

    gap_up = ((df_all['fOpen'] > gap_cond_up))
    gap_down = ((df_all['fOpen'] < gap_cond_down))
    gap_rows = df_all[gap_up | gap_down]

    df_all.loc[gap_rows.index, 'gap'] = (gap_rows[['fOpen', 'prev_close']]
                                         .pct_change(axis='columns', periods=-1)
                                         ['fOpen'].values.round(3))
    cols_to_round = ['fOpen', 'fLow', 'fClose', 'fHigh']
    df_all.dropna(subset=cols_to_round, inplace=True)
    df_all.loc[:, cols_to_round] = df_all[cols_to_round].round(3)

    return df_all


def calc_rsi(df):
    """Calculate and add RSI, overbought, oversold."""
    rsi_vals = np.array([])
    df_all_sym = df.set_index('symbol')
    df_all_sym['fClose'] = df_all_sym['fClose'].astype(np.float64)
    sym_list = (df_all_sym.index.get_level_values('symbol')
                          .unique().dropna().tolist())

    n = 0
    for symbol in tqdm(sym_list):
        try:
            prices = df_all_sym.loc[symbol]['fClose'].to_numpy()
            rsi_vals = np.append(rsi_vals, talib.RSI(prices))
        except AttributeError:  # For symbols that don't exist
            n += 1
            print(symbol)
            rsi_vals = np.append(rsi_vals, 0)
        if n > 10:  # Assume something else is wrong
            break
            return df

    rsi_vals = np.array(rsi_vals)
    df['rsi'] = rsi_vals.ravel()
    df['rsi_ob'] = np.where(df['rsi'] > 70, 1, 0)
    df['rsi_os'] = np.where(df['rsi'] < 70, 1, 0)

    return df


def make_moving_averages(df_all):
    """Make moving averages for dataframe."""
    sma_50 = []
    sma_200 = []

    df_all_sym = df_all.set_index('symbol')
    sym_list = (df_all_sym.index.get_level_values('symbol')
                          .unique().dropna().tolist())
    syms_to_exclude = []

    for sym in tqdm(sym_list):
        try:
            df_sym = df_all_sym.loc[sym]
            val_50 = df_sym['fClose'].rolling(min_periods=50, window=50).mean().to_numpy()
            val_200 = df_sym['fClose'].rolling(min_periods=200, window=200).mean().to_numpy()
            sma_50.append(val_50)
            sma_200.append(val_200)
        except AttributeError:
            syms_to_exclude.append(sym)
    # Exclude syms where moving average doesn't fit
    df_all = df_all[~df_all['symbol'].isin(syms_to_exclude)].copy()

    df_all['sma_50'] = np.concatenate(sma_50).ravel()
    df_all['sma_200'] = np.concatenate(sma_200).ravel()

    df_all['up50'] = df_all.mask('symbol', df_all['prev_symbol'])['sma_50'].diff()
    df_all['up200'] = df_all.mask('symbol', df_all['prev_symbol'])['sma_200'].diff()
    df_all['up50'] = np.where(df_all['up50'] > 0, 1, -1)
    df_all['up200'] = np.where(df_all['up200'] > 0, 1, -1)

    return df_all


def get_beta_vals(df_all=None):
    """Calculate beta values for stocks against SPY."""
    if not isinstance(df_all, pd.DataFrame):
        df_all = read_clean_combined_all(local=False)
        df_all = df_all[df_all['date'] >= '2021']

    mkt_data = (yf.download(tickers=['SPY'], period='ytd',
                            interval='1d', auto_adjust=True))

    cols_to_rename = ({'Date': 'date', 'Open': 'fOpen', 'High': 'fHigh',
                       'Low': 'fLow', 'Close': 'mktClose', 'Volume': 'fVolume'})

    df_spy = mkt_data.reset_index().rename(columns=cols_to_rename)
    df_spy.insert(1, 'symbol', 'SPY')
    df_spy['fChangeP'] = df_spy['mktClose'].pct_change(limit=1)
    df_spy['fChangeP'].fillna(method='bfill', inplace=True)

    df_all_pre = df_all[['date', 'symbol', 'fClose']].set_index(['date'])
    df_all_pivot = df_all.pivot('date', 'symbol', 'fClose').reset_index()
    df_spy_pivot = df_spy.pivot('date', 'symbol', 'mktClose').reset_index()

    for col in tqdm(df_all_pivot.columns[1:]):
        df_spy_pivot[col] = df_spy_pivot['SPY']

    df_spy_pivot.drop(columns=['SPY'], inplace=True)

    s_mkt_corr = df_all_pivot.corrwith(df_spy_pivot, axis=0, method='pearson')

    df_all_ret_piv = df_all.pivot('date', 'symbol', 'fChangeP').reset_index().drop(columns=['date'])
    df_spy_ret_piv = df_spy.pivot('date', 'symbol', 'fChangeP').reset_index().drop(columns=['date'])
    s_all_std = df_all_ret_piv.std(axis=0)
    s_mkt_std = df_spy_ret_piv.std(axis=0)

    sym_list = df_all['symbol'].unique().tolist()

    beta_pairs = []
    for sym in tqdm(sym_list):
        beta = (s_mkt_corr.loc[sym] * (s_all_std.loc[sym] / s_mkt_std.iloc[0]))
        beta_pairs.append((sym, beta))

    beta_df = pd.DataFrame(beta_pairs, columns=['symbol', 'beta'])

    return beta_df


def get_max_rows(df_sym, verb=False):
    """Get the max row."""
    col_to_pick = 'fVolume'
    df_pos = df_sym[df_sym['fChangeP'] > 0]
    max_rows = df_pos[df_pos[col_to_pick].isin(df_pos[col_to_pick].nlargest(10))].copy()
    cond = {}
    res_list = []
    max_row = None

    for index, row in max_rows.iterrows():
        prev_row, next_row = None, None
        if verb:
            print(row)

        try:
            prev_row = df_sym.loc[index - 1]
            next_row = df_sym.loc[index + 1]
        except KeyError:
            cond[index] = None
            if verb:
                print(f"KeyError Exc: next/prev row for {str(row)}")
            continue
        # If range is 0, can't divide, and prob won't use anyway
        if row['fRange'] == 0:
            cond[index] = None
            if verb:
                print(f"fRange 0: for row {str(row)}")
            continue

        cond_1 = ((row['fChangeP'] in (max_rows['fChangeP'].nlargest(5).tolist()))
                  & (not check_nan(row['fHighMax']))
                  & ((((row['fClose'] - row['fOpen']) / row['fRange'])) > .50))
        # ^ Remove candles extended tails, small body
        cond_2 = ((prev_row['fChangeP'] > 0)
                  & (next_row['fChangeP'] < 1)
                  & (next_row['fClose'] < row['fHigh'])
                  & (prev_row['fHigh'] < row['fHigh']))
        cond_3 = ((prev_row['fChangeP'] > 0) &
                  (next_row['fChangeP'] < row['fChangeP']) &
                  (next_row['fHigh'] < row['fHigh']))

        if cond_1:
            res_list.append(index)
            cond[index] = 'cond1'
            if verb:
                print(f"Symbol: {df_pos['symbol'].iloc[0]} cond_1")
        elif cond_2:
            res_list.append(index)
            cond[index] = 'cond2'
            if verb:
                print(f"Symbol: {df_pos['symbol'].iloc[0]} cond_2")
        elif cond_3:
            res_list.append(index)
            cond[index] = 'cond3'
            if verb:
                print(f"Symbol: {df_pos['symbol'].iloc[0]} cond_3")
        else:
            cond[index] = None
            pass

    if res_list:
        if verb:
            print(f"Res list: {str(res_list)}")

        if len(res_list) == 1:
            max_row = max_rows[max_rows.index == res_list[0]].copy()
            if verb:
                print(f"get_max_rows: {max_rows['symbol'].iloc[0]}: len(res_list): 1")
            max_row['res'] = '1'
        elif len(res_list) > 1:
            pos_rows = max_rows.loc[res_list[0]: res_list[-1]].copy()
            # sort1
            max_row = (pos_rows[(pos_rows['fRange'].isin(pos_rows['fRange'].nlargest(2).tolist()))
                                & (pos_rows['fChangeP'].isin(pos_rows['fChangeP'].nlargest(2).tolist()))
                                & (pos_rows['date'].isin(pos_rows['date'].nsmallest(3)))].copy())
            max_row['res'] = 'sort1'
            # sort2
            if max_row.shape[0] == 0:
                max_row = (pos_rows[(pos_rows['fChangeP'].isin(pos_rows['fChangeP'].nlargest(2).tolist()))
                                    & (pos_rows['date'].isin(pos_rows['date'].nsmallest(3).tolist()))].copy())
                max_row['res'] = 'sort2'
            # sort3
            if max_row.shape[0] in [0]:
                max_row = (pos_rows[(pos_rows['fChangeP'].isin(pos_rows['fChangeP'].nlargest(5).tolist()))
                                    & (pos_rows['gap'].isin(pos_rows['gap'].nlargest(3).tolist()))].copy())
                max_row['res'] = 'sort3'
            # sort4
            if max_row.shape[0] in [2, 3, 4, 5]:
                max_row = pos_rows[pos_rows['date'] == pos_rows['date'].min()].copy()
                max_row['res'] = 'sort4'
            if verb:
                print(f"get_max_rows: {max_rows['symbol'].iloc[0]}: len(res_list): {len(res_list)}")
            if max_row.shape[0] == 0:
                print(f"get_max_rows: no rows satisfy the res_list conditions for symbol {max_rows['symbol'].iloc[0]}")
            # sort5
            if max_row.shape[0] != 1:
                max_row = pos_rows[pos_rows['fRange'] == pos_rows['fRange'].max()].copy()
                max_row['res'] = 'sort5'
    else:
        max_row = max_rows[max_rows['fChangeP'] == max_rows['fChangeP'].max()].copy()
        cond[max_row.index[0]] = 'else'
        max_row['res'] = 'else'

    if verb:
        print(f"Condition values: {str(list(cond.values()))}")

    max_rows.loc[:, 'cond'] = list(cond.values())
    max_row.loc[:, 'cond'] = cond[max_row.index[0]]
    return max_row, max_rows


def get_rows(df_sym, max_row, verb=False, calc_date_range=False):
    """Get rows adjacent to the max row to use for analysis."""
    # Rules - there should only be one fibonacci movement
    # Limit to major runs - one to two candles
    # Start with the highest volume for the time period (coincidentally the highest % change also)
    rows = None

    n_list = list(range(50))
    # Okay so this goes one row forward and sees if conditions are met
    min_idx = max_row.index[0]
    max_idx = max_row.index[0]

    # Moving forward
    for n in n_list:
        try:
            row = df_sym.loc[max_idx + (n + 1)]

            if (row['fChangeP'] > -0.005) & (row['fHigh'] > max_row['fHigh'].iloc[0]):
                if verb:
                    print(f"Max idx: {str(max_idx + (n + 1))}")
            elif (abs(row['fChangeP']) < .0035) & (row['fHigh'] > max_row['fLow'].iloc[0]):
                if verb:
                    print(f"Max idx: {str(max_idx + (n + 1))}")
            else:
                if verb:
                    print(f"Max idx {str(row)}")
                max_idx = max_idx + n
                break
        except (KeyError, IndexError) as ke:  # When the index value isn't in the sym_df
            if verb:
                print(f"Max idx ke error: {str(ke)}")
            max_idx = max_idx + n
            break

    # Moving back
    for n in n_list:
        try:
            row = df_sym.loc[min_idx - (n + 1)]
            # If not, go one row back
            if (row['fChangeP'] >= -.005) & (row['fClose'] < max_row['fClose'].iloc[0]) & (row['fClose'] > row['fOpen']):
                # rows = pd.concat([row, max_row]).sort_values(by=['fVolume'], ascending=True)
                if verb:
                    print(f"Min idx: {str(min_idx - (n + 1))}: pos rowChangeP & fClose < max_row['fClose']")
            elif (abs(row['fChangeP']) < .0035) & (row['fHigh'] < max_row['fLow'].iloc[0]):
                if verb:
                    print(f"Min idx: {str(min_idx - (n + 1))}: pos rowChangeP & fClose < max_row['fClose']")
            elif ((max_row['fLow'].iloc[0] - row['fHigh']) > .5):
                min_idx = min_idx - (n + 1)
                if verb:
                    print(f"Min idx condition reached: {str(min_idx - (n + 1))}: max_row['fLow'] - row['fHigh'] > .5")
                break
            else:
                min_idx = min_idx - n
                if verb:
                    print(f"Min idx condition reached: {str(row)}")
                break
        except (KeyError, IndexError) as ke:
            if verb:
                print(f"Min idx ke error: {str(ke)}")
            min_idx = min_idx - n
            break

    if min_idx == max_idx:
        rows = max_row
        if verb:
            print(f"Min idx == max_idx for symbol {max_row['symbol'].iloc[0]}")
    else:
        rows = df_sym.loc[min_idx:max_idx]
        if verb:
            print(f"Max row symbol == {max_row['symbol'].iloc[0]}")
            print(f"Max row index == {max_row.index}")
            print(f"Min Idx == {min_idx}")
            print(f"Max Idx == {max_idx}")
            print(f"Rows shape {rows.shape[0]}")

    if calc_date_range:
        holidays_fpath = Path(baseDir().path, 'ref_data/holidays.parquet')
        holidays = pd.read_parquet(holidays_fpath)
        dt = getDate.query('sec_master')
        current_holidays = (holidays[(holidays['date'].dt.year >= dt.year) &
                                     (holidays['date'].dt.date <= dt)])
        hol_list = current_holidays['date'].dt.date.tolist()
        (rows.insert(2, "date_range",
         rows.apply(lambda row:
             np.busday_count(rows['date'].min().date(),
                             rows['date'].max().date(),
                             holidays=hol_list),
                axis=1)))

    return rows


def get_fib_dict(df_sym, max_row, rows, verb=False):
    """Create and return fibonacci ext/retr dict."""
    fib_percs = ([.001, .236, .382, .5, .618, .786, .999,
                  1.236, 1.5, 1.618, 2.618, 4.236])

    fib_dict = {}
    fib_dict['symbol'] = max_row['symbol'].iloc[0]
    fib_dict['cond'] = max_row['cond'].iloc[0]
    fib_dict['date'] = max_row['date'].iloc[0]
    fib_dict['start_date'] = rows['date'].min()
    fib_dict['end_date'] = rows['date'].max()
    fib_dict['start'] = rows['fLow'].min()
    fib_dict['high'] = rows['fHigh'].max()
    fib_dict['fibPercRange'] = ((fib_dict['high'] - fib_dict['start']) / fib_dict['start'])
    fib_dict['range'] = fib_dict['high'] - fib_dict['start']
    # fib_dict['ext_end'] = round(fib_dict['range'] * .50, 3)
    # fib_dict['ext_end'] = round(((max_row['fRange'].iloc[0] * .283) + max_row['fLow'].iloc[0]), 3)

    # This does work, but only for OCGN
    # fib_dict['ext_end'] = (rows['fHigh'].max() - (rows['fLow'].min() + (fib_dict['range'] * .5)))

    next_row = False
    try:
        next_row = df_sym.loc[max_row.index[0] + 1]
        if next_row['fLow'] < max_row['fLow'].iloc[0]:
            fib_dict['ext_date'] = next_row['date']

            if fib_dict['range'] > fib_dict['start']:
                fib_dict['ext_end'] = (rows['fHigh'].max() - (rows['fLow'].min() + (fib_dict['range'] * .5)))
                fib_dict['ext_cond'] = 'range>start'
            else:
                fib_dict['ext_end'] = (fib_dict['high'] - (fib_dict['range'] * .382)) * .618
                fib_dict['ext_cond'] = 'fib618'
                if fib_dict['ext_end'] < fib_dict['start']:
                    if next_row['fLow'] < max_row['fLow'].iloc[0]:
                        fib_dict['ext_date'] = next_row['date']
                        fib_dict['ext_end'] = next_row['fLow']
                        fib_dict['ext_cond'] = 'nextLow'
                    else:
                        fib_dict['ext_end'] = max_row['fLow'].iloc[0]
                        fib_dict['ext_cond'] = 'maxLow'
        else:
            if verb:
                print('get_fib_dict: KeyError raised')
            raise KeyError
    except KeyError:
        fib_dict['ext_date'] = max_row['date'].iloc[0]
        fib_dict['ext_end'] = (fib_dict['high'] - (fib_dict['range'] * .382)) * .618
        fib_dict['ext_cond'] = 'fib618'
        if fib_dict['ext_end'] < fib_dict['start']:
            fib_dict['ext_end'] = max_row['fLow'].iloc[0]
            fib_dict['ext_cond'] = 'maxLow'
        if verb:
            print('get_fib_dict: KeyError reached')

    # ret stands for retracement, ext for extension

    for fib in fib_percs:
        fib_dict[f"ret_{str(fib)}"] = round((fib_dict['high'] - (fib_dict['range'] * fib)), 3)
        fib_dict[f"ext_{str(fib)}"] = round((fib_dict['ext_end'] + (fib_dict['range'] * fib)), 3)


    return fib_dict


def get_diff_dict(fib_dict, rows, cutoff):
    """Get dict of perc diffs between stock values and fib predictions."""
    # Accuracy check
    exclude_list = (['symbol', 'date', 'cond', 'start_date', 'end_date', 'ext_date',
                     'start', 'high', 'range', 'ext_end', 'ext_cond'])
    keys = [key for key in fib_dict.keys() if '.001' not in key if '.999' not in key if key not in exclude_list]

    cols_to_check = ['fOpen', 'fHigh', 'fLow', 'fClose']
    row_vals = rows[cols_to_check].values
    # 2.5% difference
    # cutoff = .025
    diff_dict = {}
    for key in keys:
        for val in row_vals:
            calc_val = ((val - fib_dict[key]) / fib_dict[key])
            if key in diff_dict:
                diff_dict[key] = np.concatenate((diff_dict[key], calc_val))
            else:
                diff_dict[key] = calc_val

    return diff_dict


def make_confirm_df(rows, cutoff, diff_dict, fib_dict, df_confirm_all):
    """Make confirm df from acceptable values."""
    confirm_list = []
    cols_to_check = ['fOpen', 'fHigh', 'fLow', 'fClose']
    # We could even stop here. That might be enough honestly
    col_list = cols_to_check * rows.shape[0]
    symbol = rows['symbol'].iloc[0]
    dt_list = [[str(row['date'].date())] * 4 for index, row in rows.iterrows()]
    dt_list = [dt for sublist in dt_list for dt in sublist]

    for key in diff_dict.keys():
        for val, dt, col in zip(diff_dict[key], dt_list, col_list):
            if abs(val) < cutoff:
                confirm_list.append((symbol, key, dt, col, round(val, 2)))
                # print(key, dt.date(), col, round(val, 2))

    if confirm_list:
        confirm_cols = ['symbol', 'fib', 'date', 'col', 'perc_diff']
        confirm_df = pd.DataFrame(confirm_list, columns=confirm_cols)
        confirm_df['date'] = pd.to_datetime(confirm_df['date'])
        confirm_df['fib_val'] = confirm_df['fib'].map(fib_dict)
        # .round(2)
        # Append rows to df_confirm_all
        df_confirm_all = df_confirm_all.append(confirm_df)
    return df_confirm_all


def write_fibs_to_parquet(df_confirm_all, fib_dict_list):
    """Write fibonacci data to local dataframe."""
    path = Path(baseDir().path, 'studies/fibonacci', 'confirmed_all.parquet')
    df_confirm_all = dataTypes(df_confirm_all, parquet=True).df
    df_confirm_all.to_parquet(path)

    fib_df = pd.DataFrame.from_records(fib_dict_list)
    holidays_fpath = Path(baseDir().path, 'ref_data/holidays.parquet')
    holidays = pd.read_parquet(holidays_fpath)
    dt = getDate.query('sec_master')
    current_holidays = (holidays[(holidays['date'].dt.year >= dt.year) &
                                 (holidays['date'].dt.date <= dt)])
    hol_list = current_holidays['date'].dt.date.tolist()
    (fib_df.insert(2, "date_range",
     fib_df.apply(lambda row:
         np.busday_count(row['start_date'].date(),
                         row['end_date'].date(),
                         holidays=hol_list),
            axis=1)))
    fib_df = dataTypes(fib_df, parquet=True).df
    path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals.parquet')
    fib_df.to_parquet(path)

# %% codecell
