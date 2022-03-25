"""Get stats for iex symbols."""
# %% codecell
from pathlib import Path

import pandas as pd

try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.pathClasses.construct_paths import PathConstructs
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
    from multiuse.pathClasses.construct_paths import PathConstructs

# %% codecell


def execute_iex_stats(df, testing=False):
    """Task_functions loop for individual bin."""
    # Df is in json format because it's being passed from a celery task
    df = pd.read_json(df)
    dt = getDate.query('iex_eod')

    for index, row in df.iterrows():
        try:
            get_daily_stats(row, dt)
        except Exception as e:
            help_print_arg(f"Daily Stats Error: symbol - {row['symbol']} - {str(e)}")
        if testing:
            break


def get_daily_stats(row, dt):
    """Get and store stock meta stats."""
    stats = urlData(row['url']).df

    # Add date column
    stats['date'] = dt
    # Base path
    bpath = Path(baseDir().path, 'company_stats/stats')
    fpath = PathConstructs.hist_sym_fpath(row['symbol'], bpath, dt)

    write_to_parquet(stats, fpath, combine=True)

# %% codecell
