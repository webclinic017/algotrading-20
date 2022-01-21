"""Iex Intraday Multiple Calendar Days."""
# %% codecell
from datetime import timedelta

try:
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import getDate
    from api import serverAPI

# %% codecell


def get_last_30_intradays():
    """Get last 30 intraday trading days."""
    bsdays = getDate.get_bus_days()

    dt_today = getDate.query('iex_eod')
    dt_30 = dt_today - timedelta(days=30)

    days = (bsdays[(bsdays['date'].dt.date >= dt_30)
            & (bsdays['date'].dt.date <= dt_today)])

    df_m1 = serverAPI('iex_intraday_m1').df
    days_tget = (days[~days['date'].isin(df_m1['date']
                 .unique())].copy())
    days_tget['dt_fmt'] = days_tget['date'].dt.strftime('%Y%m%d')

    try:
        from app.tasks import execute_func
        for dt in days_tget['dt_fmt']:
            kwargs = {'dt': dt}
            execute_func.delay('iex_intraday', **kwargs)
    except ModuleNotFoundError:
        pass

# %% codecell
