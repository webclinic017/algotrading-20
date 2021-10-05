"""For getting option data from yfinance."""
# %% codecell
from pathlib import Path

import pandas as pd
import yfinance as yf
from socks import SOCKS5AuthError

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, help_print_arg

# %% codecell


def execute_yahoo_options(df):
    """Execute for loop. Run from tasks execute_function."""
    # Df is in json format because it's being passed from a celery task
    df = pd.read_json(df)
    for index, row in df.iterrows():
        try:
            yahoo_options(row['symbol'], proxy=row['proxy'])
        except (SOCKS5AuthError, TypeError) as te:
            # help_print_arg(index)
            help_print_arg(str(te))
            path = Path(baseDir().path, 'derivatives/end_of_day/unfinished', f"df_bin{row['bins']}.parquet")
            df.iloc[index:].to_parquet(path)
            break
        except Exception as e:
            help_print_arg(str(e))
            yahoo_options(row['symbol'], proxy=row['proxy'])


def yahoo_options(sym, proxy=False, n=False, temp=True):
    """Get options chain data from yahoo finance."""
    dt = getDate.query('iex_eod')
    yr, fpath = dt.year, ''
    fpath_base = Path(baseDir().path, 'derivatives/end_of_day')

    # If writing to the temporary directory for today's data
    if not temp:
        fpath = Path(fpath_base, str(yr), sym.lower()[0], f"_{sym}.parquet")
    elif temp:
        fpath = Path(fpath_base, 'temp', str(yr), sym.lower()[0], f"_{sym}.parquet")

    if fpath.is_file():
        df_old = pd.read_parquet(fpath)

    ticker = yf.Ticker(sym)
    exp_dates = ticker.options

    if n:
        n += (len(exp_dates) + 1) * 2
    df_list, chain = [], False

    if proxy:
        chain = ticker.option_chain(proxy=proxy)
    else:
        chain = ticker.option_chain()

    try:
        df_calls = chain.calls
        df_calls['symbol'] = sym
        df_puts = chain.puts
        df_puts['symbol'] = sym
        df_list.append(df_calls)
        df_list.append(df_puts)

    except Exception as e:
        # error_list.append(sym)
        help_print_arg(str(e))
        break

    try:
        df_all = pd.concat(df_list)

        df_all['date'] = pd.to_datetime(dt)
        df_all['symbol'] = sym
        df_all = df_all.set_index('symbol')

        if fpath.is_file():
            df_old = pd.read_parquet(fpath)
            df_all = pd.concat([df_old, df_all])
            df_all.to_parquet(fpath)
        else:
            df_all.to_parquet(fpath)

    except ValueError:
        # empty_list.append(sym)
        pass



# %% codecell
