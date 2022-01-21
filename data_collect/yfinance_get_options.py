"""For getting option data from yfinance."""
# %% codecell
from pathlib import Path
import time
import pandas as pd
import yfinance as yf
from socks import SOCKS5AuthError

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet

# %% codecell


def execute_yahoo_options(df, verbose=False):
    """Execute for loop. Run from tasks execute_function."""
    # Df is in json format because it's being passed from a celery task
    df = pd.read_json(df)

    # Add all index/row errors to dict for future use
    error_dict = {}

    for index, row in df.iterrows():
        try:
            yahoo_options(row['symbol'], proxy=row['proxy'])
        except SOCKS5AuthError as sae:
            # Print error
            if verbose:
                help_print_arg(str(sae))
            try:
                time.sleep(.5)
                yahoo_options(row['symbol'], proxy=row['proxy'])
            except Exception as e:  # End loop
                break
        except TypeError as te:
            error_dict[index] = row
            if verbose:
                help_print_arg(str(te))
        except Exception as e:
            error_dict[index] = row
            if verbose:
                help_print_arg(str(e))

    try:
        # Create dataframe from error dict
        df_errors = pd.DataFrame.from_dict(error_dict).T
        df_unfin = pd.concat([df_errors, df.iloc[index:]]).copy()
        # Define path to write file
        path = Path(baseDir().path, 'derivatives/end_of_day/unfinished', f"df_bin{row['bins']}.parquet")
        df_unfin.to_parquet(path)
    except UnboundLocalError:
        pass



def yahoo_options(sym, proxy=False, n=False, temp=True, verbose=False):
    """Get options chain data from yahoo finance."""
    dt = getDate.query('iex_eod')
    yr, fpath = dt.year, ''
    fpath_base = Path(baseDir().path, 'derivatives/end_of_day')

    # If writing to the temporary directory for today's data
    if not temp:
        fpath = Path(fpath_base, str(yr), sym.lower()[0], f"_{sym}.parquet")
    elif temp:
        fpath = Path(fpath_base, 'temp', str(yr), sym.lower()[0], f"_{sym}.parquet")

    # print(fpath)

    if fpath.is_file():
        df_old = pd.read_parquet(fpath)

    ticker = yf.Ticker(sym)

    if n:
        exp_dates = ticker.options
        n += (len(exp_dates) + 1) * 2
    df_list, chain = [], False

    if proxy:
        chain = ticker.option_chain(proxy=proxy)
    else:
        chain = ticker.option_chain()

    try:
        df_calls = chain.calls
        df_puts = chain.puts
        df_list.append(df_calls)
        df_list.append(df_puts)

    except Exception as e:
        # error_list.append(sym)
        chain = ticker.option_chain()
        df_calls = chain.calls
        df_puts = chain.puts
        df_list.append(df_calls)
        df_list.append(df_puts)
        if verbose:
            help_print_arg(str(e))

    try:
        df_all = pd.concat(df_list)

        df_all['date'] = pd.to_datetime(dt)
        df_all['symbol'] = sym
        df_all = df_all.set_index('symbol')

        if fpath.is_file():
            df_old = pd.read_parquet(fpath)
            df_all = pd.concat([df_old, df_all])
            df_all.drop_duplicates(subset=['contractSymbol', 'date'], inplace=True)
            df_all.to_parquet(fpath)
        else:
            df_all.to_parquet(fpath)

    except ValueError:
        # empty_list.append(sym)
        pass



# %% codecell
