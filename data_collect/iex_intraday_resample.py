"""Resampling IEX intraday data."""
# %% codecell
from pathlib import Path

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from api import serverAPI

# %% codecell


def resample_to_min(df, period='3T', reset_index=True):
    """Resampling intraday data from 1 min to x min."""

    if df.index.name != 'dtime':
        df = df.set_index('dtime')

    resamp = df.resample(period)

    cols_to_open = ['open', 'marketOpen']
    cols_to_close = (['close', 'marketClose', 'changeOverTime',
                      'marketChangeOverTime'])
    cols_to_high = ['high', 'marketHigh']
    cols_to_low = ['low', 'marketLow']
    cols_to_sum = (['volume', 'notional', 'numberOfTrades', 'marketVolume',
                    'marketNotional', 'marketNumberOfTrades'])

    df[cols_to_open] = resamp[cols_to_open].first()
    df[cols_to_close] = resamp[cols_to_close].last()
    df[cols_to_high] = resamp[cols_to_high].max()
    df[cols_to_low] = resamp[cols_to_low].min()
    df[cols_to_sum] = resamp[cols_to_sum].sum()

    df['average'] = df['notional'].div(df['volume'])
    df['marketAverage'] = df['marketNotional'].div(df['marketVolume'])

    df_per = df.resample(period).asfreq().copy()

    if reset_index:
        df_per = df_per.reset_index()

    return df_per


class ResampleIntraday():
    """Resample from 1 minute intraday data."""

    """
    self.fpaths : all fpaths for 1 minute data files
    self.dt : dt
    """

    def __init__(self, dt=None):
        self._get_fpaths(self, dt)
        self._start_fpaths_loop(self)

    @classmethod
    def _get_fpaths(cls, self, dt):
        """Get list of all fpaths in 1 minute intraday."""
        if not dt:
            dt = getDate.query('iex_eod')

        year = dt.year
        bpath = Path(baseDir().path, 'intraday', 'minute_1', str(year))
        fpaths = list(bpath.glob('**/*.parquet'))

        self.fpaths = fpaths
        self.dt = dt

    @classmethod
    def _start_fpaths_loop(cls, self):
        """Start the for loop for each fpath."""
        bpath = Path(baseDir().path, 'intraday')

        for fpath in self.fpaths:
            self._resample_and_write(self, fpath, bpath, dt=self.dt)

    @classmethod
    def _resample_and_write(cls, self, fpath, bpath, dt):
        """Resample from 1 to 3,5 minute, write to local df file."""
        df = pd.read_parquet(fpath)
        sym = fpath.split('_')[-1].split('.')[0]

        fpre = sym[0].lower()
        fsuf = f"_{sym}.parquet"

        fpath_3 = bpath.joinpath('minute_3', str(dt.year), fpre, fsuf)
        fpath_5 = bpath.joinpath('minute_5', str(dt.year), fpre, fsuf)

        df_3 = resample_to_min(df, period='3T')
        df_5 = resample_to_min(df, period='5T')

        write_to_parquet(df_3, fpath_3)
        write_to_parquet(df_5, fpath_5)

# %% codecell
