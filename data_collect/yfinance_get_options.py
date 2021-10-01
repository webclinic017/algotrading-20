"""For getting option data from yfinance."""
# %% codecell
from pathlib import Path

import pandas as pd
import yfinance as yf


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
        yahoo_options(row['symbol'], proxy=row['proxy'])


def yahoo_options(sym, proxy=False, n=False):
    """Get options chain data from yahoo finance."""
    dt = getDate.query('iex_eod')
    yr = dt.year
    fpath_base = Path(baseDir().path, 'derivatives/end_of_day', str(yr))
    fpath = Path(fpath_base, sym.lower()[0], f"_{sym}.parquet")

    if fpath.is_file():
        df_old = pd.read_parquet(fpath)

    ticker = yf.Ticker(sym)
    exp_dates = ticker.options
    if n:
        n += (len(exp_dates) + 1) * 2
    df_list = []

    for exp in exp_dates:
        try:
            if proxy:
                options = ticker.option_chain(exp, proxy=proxy)
            else:
                options = ticker.option_chain(exp)
            df_calls = options.calls
            df_calls['side'] = 'C'
            df_puts = options.puts
            df_puts['side'] = 'P'
            df_list.append(df_calls)
            df_list.append(df_puts)
        except Exception as e:
            # error_list.append(sym)
            help_print_arg(e)
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
