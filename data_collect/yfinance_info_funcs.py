"""Yfinance functions for collecting meta information."""
# %% codecell
from pathlib import Path
from socks import SOCKS5AuthError
import time

import yfinance as yf
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, help_print_arg


# %% codecell


def execute_yahoo_func(df, which='yinfo', verbose=False, **kwargs):
    """Execute for loop. Run from tasks execute_function."""
    # Df is in json format because it's being passed from a celery task
    df = pd.read_json(df)

    # If which function to execute is passed
    if 'which' in kwargs.keys():
        which = kwargs['which']

    # Define function dict and unfinished fpath dir to store unfinished symbols
    func_dict = {'yinfo': ysymbols_info}
    unfin_dict = ({'yinfo': 'tickers/info/unfinished',
                   'yoptions': 'derivatives/end_of_day/unfinished'})

    # Add all index/row errors to dict for future use
    error_dict = {}

    for index, row in df.iterrows():
        try:
            if which == 'yinfo':
                func_dict[which](row['symbol'])
        except SOCKS5AuthError as sae:
            # Print error
            help_print_arg(f"Execute Yahoo Func: Socks 5 AuthError: {str(sae)}")
            try:
                time.sleep(.5)
                if which == 'yinfo':
                    func_dict[which](row['symbol'])
            except Exception as e:  # End loop
                break
        except TypeError as te:
            error_dict[index] = row
            if verbose:
                help_print_arg(f"Execute yahoo func: TypeError: {str(te)}")
                help_print_arg(f"{str(index)}: {str(row)}")
        except Exception as e:
            error_dict[index] = row
            if verbose:
                help_print_arg(f"Execute yahoo func: Gen Excp: {str(e)}")

    try:
        # Create dataframe from error dict
        df_errors = pd.DataFrame.from_dict(error_dict).T
        df_unfin = pd.concat([df_errors, df.iloc[index:]]).copy()
        # Define path to write file
        path = Path(baseDir().path, unfin_dict[which], f"df_bin{row['bins']}.parquet")
        df_unfin.to_parquet(path)
    except UnboundLocalError:
        pass


# %% codecell


def ysymbols_info(sym, base_dir=False, session=False, testing=False, verbose=False):
    """Get meta information for all ysymbols."""
    df_old, info = False, False

    if not base_dir:  # Check if base_dir has been passed in function
        dt = getDate.query('iex_eod')
        yr = dt.year
        base_dir = Path(baseDir().path, 'tickers/info', str(yr))

    path = Path(base_dir, sym[0].lower(), f"_{sym}.parquet")

    if testing:  # If testing, print local path
        help_print_arg(path)

    ticker = yf.Ticker(sym)

    if session:
        try:
            info = ticker.info(session=session)
        except TypeError as te:
            if verbose:
                help_print_arg(f"TypeError (proxy) from ysymbols_info: {te}")
    else:
        info = ticker.info

    try:
        # Convert single row dict into dataframe
        df_new = pd.DataFrame(info.items()).set_index(0).T
        # See if there's already a file, that we need to concat
        if path.exists():
            df_old = pd.read_parquet(path)
            df_all = pd.concat([df_new, df_old])
            df_all.to_parquet(path)
            # df_all = dataTypes(df_all, parquet=True)
        else:  # If not df_old, write file to local parquet file
            df_new.to_parquet(path)
    except Exception as e:
        if verbose:
            help_print_arg(f"Symbol {sym} with error {str(e)}")


# %% codecell
