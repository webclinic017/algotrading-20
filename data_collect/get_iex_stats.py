"""Get stats for iex symbols."""
# %% codecell
from pathlib import Path

import pandas as pd

try:
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from data_collect.iex_class import urlData
    from multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet

# %% codecell


def execute_iex_stats(df, testing=False):
    """Task_functions loop for individual bin."""
    # Df is in json format because it's being passed from a celery task
    df = pd.read_json(df)

    for index, row in df.iterrows():
        try:
            get_daily_stats(row)
        except Exception as e:
            help_print_arg(f"Daily Stats Error: symbol - {row['symbol']} - {str(e)}")
        if testing:
            break


def get_daily_stats(row):
    """Get and store stock meta stats."""
    stats = urlData(row['url']).df

    dt = getDate.query('iex_eod')
    # Add date column
    stats['date'] = dt
    base_path = Path(baseDir().path, 'company_stats/stats', str(dt.year))
    path = Path(base_path, row['symbol'].lower()[0], f"_{row['symbol']}.parquet")

    if path.exists():
        old_df = pd.read_parquet(path)
        df_all = pd.concat([old_df, stats]).reset_index(drop=True)
        write_to_parquet(df_all, path)
    else:
        write_to_parquet(stats, path)

# %% codecell
