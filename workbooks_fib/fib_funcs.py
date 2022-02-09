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
    from scripts.dev.studies.add_study_cols import add_gap_col, calc_rsi, make_moving_averages, add_fChangeP_col, add_fHighMax_col
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.df_helpers import DfHelpers
    from scripts.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.multiuse.pd_funcs import mask, chained_isin
    from scripts.dev.api import serverAPI
    from scripts.dev.multiuse.symbol_ref_funcs import remove_funds_spacs
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from studies.add_study_cols import add_gap_col, calc_rsi, make_moving_averages, add_fChangeP_col, add_fHighMax_col
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.df_helpers import DfHelpers
    from multiuse.create_file_struct import makedirs_with_permissions
    from multiuse.path_helpers import get_most_recent_fpath
    from multiuse.pd_funcs import mask, chained_isin
    from api import serverAPI
    from multiuse.symbol_ref_funcs import remove_funds_spacs

# %% codecell
pd.DataFrame.mask = mask
pd.DataFrame.chained_isin = chained_isin

# %% codecell


def read_clean_combined_all(local=False, dt=None, filter_syms=True):
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
        cols_to_read = ['date', 'symbol', 'fOpen', 'fHigh', 'fLow', 'fClose', 'fVolume']
        df_all = serverAPI('stock_close_cb_all').df
        df_all = df_all[cols_to_read]

        if filter_syms:
            all_cs_syms = remove_funds_spacs()
            df_all = df_all[df_all['symbol'].isin(all_cs_syms['symbol'])].copy()

        df_all['date'] = pd.to_datetime(df_all['date'])

        # Define base bpath for 2015-2020 stock data
        bpath = Path(baseDir().path, 'historical/each_sym_all')
        path = get_most_recent_fpath(bpath.joinpath('each_sym_all', 'combined_all'))
        df_hist = pd.read_parquet(path)
        # Combine 2015-2020 stock data with ytd
        df_all = pd.concat([df_hist, df_all]).copy()

        df_all.drop_duplicates(subset=['symbol', 'date'], inplace=True)
        df_all.reset_index(drop=True, inplace=True)

    if not dt:
        dt = getDate.query('iex_eod')
    # Exclude all dates from before this year
    df_all = (df_all[df_all['date'] >= str(dt.year)]
              .dropna(subset=['fVolume'])
              .copy())

    # Get rid of all symbols that only have 1 day of data
    df_vc = df_all['symbol'].value_counts()
    df_vc_1 = df_vc[df_vc == 1].index.tolist()
    df_all = (df_all[~df_all['symbol'].isin(df_vc_1)]
              .reset_index(drop=True).copy())
    # Sort by symbol, date ascending
    df_all = df_all.sort_values(by=['symbol', 'date'], ascending=True)

    df_all['fRange'] = (df_all['fHigh'] - df_all['fLow'])
    df_all['vol/mil'] = (df_all['fVolume'].div(1000000))
    df_all['prev_close'] = df_all['fClose'].shift(periods=1, axis=0)
    df_all['prev_symbol'] = df_all['symbol'].shift(periods=1, axis=0)

    # Add fChangeP col
    print('Fib funcs: adding fChangeP column')
    df_all = add_fChangeP_col(df_all)

    # Merge with df_all and resume

    # Add gap column
    print('Fib funcs: adding gap column')
    df_all = add_gap_col(df_all)

    # Add range of gap
    df_all['gRange'] = (np.where(df_all['prev_close'] < df_all['fLow'],
                                 df_all['fHigh'] - df_all['prev_close'],
                                 df_all['fHigh'] - df_all['fLow']))

    df_all['cumPerc'] = np.where(
        df_all['symbol'] == df_all['prev_symbol'],
        df_all['fChangeP'].cumsum(),
        np.NaN)

    df_all['perc5'] = np.where(
        df_all['symbol'] == df_all['prev_symbol'],
        df_all['cumPerc'].shift(-5) - df_all['cumPerc'],
        np.NaN)

    df_all['vol_avg_2m'] = np.where(
        df_all['symbol'] == df_all['prev_symbol'],
        df_all['fVolume'].rolling(60).mean(),
        np.NaN)

    # Add cumulative sum of last 5 fChangeP rows
    df_all['fCP5'] = (np.where(df_all['symbol'] == df_all['prev_symbol'],
                      df_all['fChangeP'].rolling(min_periods=1, window=5).sum(),
                      0))


    df_all = df_all.copy()
    # Calc RSI and moving averages
    print('Fib Funcs: calc_rsi')
    df_all = calc_rsi(df_all)
    print('Fib Funcs: making_moving_averages')
    df_all = make_moving_averages(df_all)

    # fHighMax funcs
    print('Fib funcs: fHighMax')
    df_all = add_fHighMax_col(df_all).copy()


    df_all = df_all.sort_values(by=['symbol', 'date'], ascending=True)

    float_32s = df_all.dtypes[df_all.dtypes == np.float32].index
    for col in float_32s:
        df_all[col] = df_all[col].astype(np.float64).round(3)

    df_all = dataTypes(df_all, parquet=True).df.copy()

    return df_all


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
                  & (not DfHelpers.check_nan(row['fHighMax']))
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
            row_pre = df_sym.loc[min_idx - (n)]
            # If not, go one row back
            if (row['fChangeP'] >= -.005) & (row['fClose'] < max_row['fClose'].iloc[0]) & (row['fClose'] > row['fOpen']):
                # rows = pd.concat([row, max_row]).sort_values(by=['fVolume'], ascending=True)
                if verb:
                    print(f"Min idx: {str(min_idx - (n + 1))}: pos rowChangeP & fClose < max_row['fClose']")
            elif ((abs(row['fChangeP']) < .0035)
                  & (row['fHigh'] * .995 < max_row['fLow'].iloc[0])
                  & (row['fHigh'] > row_pre['fHigh'])
                  & (row['fLow'] > row_pre['fLow'])):
                if verb:
                    print(f"Min idx: {str(min_idx - (n + 1))}: row['fHigh'] * .995 < max_row['fLow']")
            elif (((max_row['fLow'].iloc[0] > row['fLow'])
                  & (max_row['fClose'].iloc[0] > row['fHigh']))
                  & (row['fHigh'] > row_pre['fHigh'])
                  & (row['fLow'] > row_pre['fLow'])):
                if verb:
                    print(f"Min idx: {str(min_idx - (n + 1))}: max_row['fLow'].iloc[0] > row['fLow']")
            elif ((max_row['fLow'].iloc[0] > row['fLow']) & (max_row['fHigh'].iloc[0] < row['fOpen'])):
                min_idx = min_idx - (n + 1)
                if verb:
                    print(f"Min idx condition reached: {str(min_idx - (n + 1))}: max_row['fHigh'] < row['fOpen']")
                break
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
                  1.236, 1.5, 1.618, 2.0, 2.618, 3.0, 4.236])

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
    write_to_parquet(df_confirm_all, path)

    fib_df = pd.DataFrame.from_records(fib_dict_list)
    (fib_df.insert(2, "date_range",
     getDate.get_bus_day_diff(fib_df, 'start_date', 'end_date')))
    path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals_test.parquet')
    write_to_parquet(fib_df, path)


# %% codecell
