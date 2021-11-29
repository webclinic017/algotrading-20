"""Functions for fibonacci sequence analysis."""
# %% codecell
import os
import sys
from pathlib import Path
from io import BytesIO
import importlib
from tqdm import tqdm

import datetime

import pandas as pd
import numpy as np


try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.create_file_struct import makedirs_with_permissions
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions
    from multiuse.path_helpers import get_most_recent_fpath
    from api import serverAPI

# %% codecell


def read_clean_combined_all(local=False):
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
    # .astype('uint32')
    df_all = df_all.sort_values(by=['symbol', 'date'], ascending=True)

    return df_all


def get_max_rows(df_sym, verb=False):
    """Get the max row."""
    col_to_pick = 'vol/mil'
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

        cond_1 = ((row['fHigh'] in (max_rows['fHigh'].nsmallest(3)))
                   & (row['date'] in (max_rows['date'].nsmallest(2))))
        cond_2 = ((prev_row['fChangeP'] > 0)
                  & (next_row['fChangeP'] < 1)
                  & (next_row['fHigh'] < row['fHigh']))
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
            print(res_list)

        if len(res_list) == 1:
            max_row = max_rows[max_rows.index == res_list[0]].copy()
            if verb:
                print(f"get_max_rows: {max_rows['symbol'].iloc[0]}: len(res_list): 1")
        elif len(res_list) > 1:
            pos_rows = max_rows.loc[res_list[0]: res_list[-1]]
            max_row = pos_rows[pos_rows['date'] == pos_rows['date'].min()].copy()

            if verb:
                print(f"get_max_rows: {max_rows['symbol'].iloc[0]}: len(res_list): {len(res_list)}")
    else:
        max_row = max_rows[max_rows['fChangeP'] == max_rows['fChangeP'].max()].copy()
        cond[max_row.index[0]] = 'else'

    max_rows.loc[:, 'cond'] = list(cond.values())
    max_row.loc[:, 'cond'] = cond[max_row.index[0]]
    return max_row, max_rows


def get_rows(df_sym, max_row, verb=False):
    """Get rows adjacent to the max row to use for analysis."""
    # Rules - there should only be one fibonacci movement
    # Limit to major runs - one to two candles
    # Start with the highest volume for the time period (coincidentally the highest % change also)
    rows = None

    n_list = list(range(50))
    # Okay so this goes one row forward and sees if conditions are met
    min_idx = max_row.index[0]
    max_idx = max_row.index[0]

    for n in n_list:
        try:
            row = df_sym.loc[max_idx + (n + 1)]

            if (row['fChangeP'] > -0.005) & (row['fHigh'] > max_row['fHigh'].iloc[0]):
                if verb:
                    print(f"Max idx: {str(max_idx + (n + 1))}")
                pass
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

    for n in n_list:
        try:
            row = df_sym.loc[min_idx - (n + 1)]
            # If not, go one row back
            if (row['fChangeP'] >= 0) & (row['fClose'] < max_row['fClose'].iloc[0]):
                # rows = pd.concat([row, max_row]).sort_values(by=['fVolume'], ascending=True)
                if verb:
                    print(f"Min idx: {str(min_idx - (n + 1))}: pos rowChangeP & fClose < max_row['fClose']")
                pass
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
            else:
                fib_dict['ext_end'] = (fib_dict['high'] - (fib_dict['range'] * .382)) * .618
                if fib_dict['ext_end'] < fib_dict['start']:
                    if next_row['fLow'] < max_row['fLow'].iloc[0]:
                        fib_dict['ext_date'] = next_row['date']
                        fib_dict['ext_end'] = next_row['fLow']
                    else:
                        fib_dict['ext_end'] = max_row['fLow'].iloc[0]
        else:
            if verb:
                print('get_fib_dict: KeyError raised')
            raise KeyError
    except KeyError:
        fib_dict['ext_date'] = max_row['date'].iloc[0]
        fib_dict['ext_end'] = (fib_dict['high'] - (fib_dict['range'] * .382)) * .618
        if fib_dict['ext_end'] < fib_dict['start']:
            fib_dict['ext_end'] = max_row['fLow'].iloc[0]
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
                     'start', 'high', 'range', 'ext_end'])
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
