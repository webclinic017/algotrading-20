"""Get a dataframe of missing dates from input df."""
# %% codecell
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate
except ModuleNotFoundError:
    from multiuse.help_class import getDate

# %% codecell


def get_missing_dates(df):
    """Get missing dates from data frame."""
    # Need columns for symbol, and date, or some other unique identifier.
    bus_days = getDate.get_bus_days(this_year=True)
    dt = getDate.query('iex_eod')
    bus_days = bus_days[bus_days['date'].dt.date <= dt].copy()

    df_dt_list = df['date'].unique()
    dts_missing = bus_days[~bus_days['date'].isin(df_dt_list)].copy()
    dts_missing['dt_format'] = dts_missing['date'].dt.strftime('%Y%m%d')

    return dts_missing


# %% codecell
