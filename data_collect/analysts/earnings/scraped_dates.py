"""Function for past 1yr of scraped analyst earnings estimates."""
# %% codecell

from pathlib import Path
import os
from time import sleep
from random import randint

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, help_print_arg
    from scripts.dev.data_collect.analysts.earnings.analyst_earnings_ests import ScrapedEE
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, help_print_arg
    from data_collect.analysts.earnings.analyst_earnings_ests import ScrapedEE

# %% codecell


def scraped_ee_dates(verbose=False, hist=False, current_year=True):
    """Start for loop of dates to get future/past analyst estimates."""
    dt = getDate.query('iex_eod')
    bdays, pos_days = None, None

    if (365 - dt.timetuple().tm_yday) > 15:
        bdays = getDate.get_bus_days(this_year=True)
    else:
        bdays = getDate.get_bus_days(this_year=False)
        bdays = bdays[bdays['date'].dt.year >= dt.year].copy()

    bdays['current_date'] = pd.to_datetime(getDate.query('iex_close'))
    bdays['bday_diff'] = (getDate.get_bus_day_diff(
                          bdays, 'current_date', 'date'))

    if hist and not current_year:
        pos_days = bdays[bdays['bday_diff'] < 15].copy()
    elif hist and current_year:
        cond1 = (bdays['bday_diff'] < 15)
        cond2 = (bdays['date'].dt.year == dt.year)
        pos_days = bdays[cond1 & cond2].copy()
    else:
        pos_days = bdays[bdays['bday_diff'].between(0, 15)].copy()

    bpath = Path(baseDir().path, 'economic_data', 'analyst_earnings')
    fpath_dir = bpath.joinpath(f"_{str(dt.year)}")

    pos_days['fpath'] = (pos_days.apply(lambda row:
                         f"{fpath_dir}/_{str(row['date'].date())}.parquet",
                                        axis=1))

    pos_days['fpath_exists'] = (pos_days['fpath'].astype(str)
                                .map(os.path.exists))
    dt_need = pos_days[~pos_days['fpath_exists']]

    dt_list = []

    for dt in dt_need['date']:
        try:
            ScrapedEE(dt=dt.date())
            sleep(randint(5, 15))
            dt_list.append(dt.date())
        except Exception as e:
            help_print_arg(f"scraped_ee_dates {type(e)} {str(e)}")

    if verbose:
        help_print_arg(str(dt_list))


# %% codecell
