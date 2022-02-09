"""Fib funcs master sequence."""
# %% codecell
from pathlib import Path

import pandas as pd
from tqdm import tqdm

try:
    from scripts.dev.multiuse.help_class import write_to_parquet, baseDir, help_print_arg
    from scripts.dev.workbooks_fib.fib_funcs import (
        get_max_rows, get_rows,
        get_fib_dict, get_diff_dict,
        make_confirm_df, read_clean_combined_all,
        write_fibs_to_parquet
    )
except ModuleNotFoundError:
    from multiuse.help_class import write_to_parquet, baseDir, help_print_arg
    from workbooks_fib.fib_funcs import (
        get_max_rows, get_rows,
        get_fib_dict, get_diff_dict,
        make_confirm_df, read_clean_combined_all,
        write_fibs_to_parquet
    )

# %% codecell


def fib_master(dt=None, verbose=False):
    """Full fibonacci sequence function."""

    df_all = read_clean_combined_all(local=False, dt=dt)

    mrow_empty_list = []
    mrows_empty_list = []

    confirm_cols = ['symbol', 'fib', 'date', 'col', 'perc_diff']
    df_confirm_all = pd.DataFrame(columns=confirm_cols)
    fib_dict_list = []
    mrows_list = []
    cutoff = .025
    symbol_list = df_all['symbol'].unique().tolist()

    n = 0

    for symbol in tqdm(symbol_list):
        df_sym = (df_all[df_all['symbol'] == symbol]
                  .sort_values(by=['date'], ascending=True)
                  .reset_index(drop=True)
                  .copy())
        # Onto the next iteration if less than 50 days of data
        if df_sym.shape[0] < 15:
            continue

        max_row, max_rows = None, None
        try:
            max_row, max_rows = get_max_rows(df_sym, verb=False)
        except IndexError:
            continue
        # If high or range is 0
        if (max_row['fHigh'].iloc[0] == 0) or (max_row['fRange'].iloc[0] == 0):
            continue
        # I'd like to see all max_row possibilities
        if not max_rows.empty:
            mrows_list.append(max_rows)


        rows = get_rows(df_sym, max_row, verb=False)
        # If rows is empty
        if rows is None:
            continue
        elif rows.empty:
            print(f"rows for symbol: {df_sym['symbol'].iloc[0]} is empty!")
            mrow_empty_list.append(max_row)
            mrows_empty_list.append(max_rows)
            continue

        fib_dict = get_fib_dict(df_sym, max_row, rows, verb=False)
        # Add fib dict to list
        fib_dict_list.append(fib_dict)
        diff_dict = get_diff_dict(fib_dict, rows, cutoff)
        df_confirm_all = (make_confirm_df(rows, cutoff, diff_dict,
                                          fib_dict, df_confirm_all))

        n += 1
        if n > 100000:
            break

    if verbose:
        help_print_arg(f"fib_master: len df_confirm_all {str(df_confirm_all.shape[0])}")
        help_print_arg(f"fib_master: len fib_dict_list {str(len(fib_dict_list))}")

    if len(symbol_list) > 50:
        write_fibs_to_parquet(df_confirm_all, fib_dict_list)
        df_mrows = pd.concat(mrows_list).reset_index()
        bpath = Path(baseDir().path, 'studies/fibonacci')
        path = bpath.joinpath('mrow_options.parquet')
        df_mrows['cond'] = df_mrows['cond'].astype('str')

        write_to_parquet(df_mrows, path)


# %% codecell
