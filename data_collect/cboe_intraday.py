"""Get intraday cboe data."""
# %% codecell
from pathlib import Path
from io import BytesIO

import pandas as pd
import numpy as np
import requests

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet

# %% codecell


class CboeIntraday():
    """Get cboe intraday data."""

    """
    self.dt : date to use
    self.bpath : base path to use for reading/writing
    self.df : df_all (no timestamps)
    """

    def __init__(self):
        df_all = self._get_cboe_data(self)
        self._write_cboe_eod(self, df_all)
        self._write_cboe_intraday(self, df_all)

    @classmethod
    def _get_cboe_data(cls, self):
        """Get symbol volume data from cboe."""
        markets = ['cone', 'opt', 'ctwo', 'exo']
        url_base = 'https://www.cboe.com/us/options/market_statistics/symbol_data/csv/?mkt'
        # Empty data list to append, then concat dataframes to
        df_list = []
        # Iterate through market list and get data for each one
        for mar in markets:
            url = f"{url_base}={mar}"
            get = requests.get(url)
            if get.status_code == 200:
                df = pd.read_csv(BytesIO(get.content))
                df['exch'] = mar
                df_list.append(df)
        # Concatenate data for all 4 markets
        df_all = pd.concat(df_list)
        dt = getDate.query('iex_close')
        df_all.insert(1, "date", dt)
        df_all['date'] = pd.to_datetime(df_all['date'])
        df_all['Expiration'] = pd.to_datetime(df_all['Expiration'])

        self.dt = dt
        self.bpath = Path(baseDir().path, 'derivatives/cboe_intraday', str(dt.year))
        self.df = df_all

        return df_all

    @classmethod
    def _write_cboe_eod(cls, self, df_all):
        """Write cboe cum sum (eod) data to local file."""
        path_eod = self.bpath.joinpath(f"_{self.dt}_eod.parquet")
        write_to_parquet(df_all, path_eod)

    @classmethod
    def _write_cboe_intraday(cls, self, df_all):
        """Write cboe intraday data to local file."""
        df_all['time'] = pd.Timestamp.today(tz='US/Eastern')
        path_intra = self.bpath.joinpath(f"_{self.dt}_intraday.parquet")
        if path_intra.exists():
            df_old = pd.read_parquet(path_intra)
            df_all = pd.concat([df_old, df_all]).reset_index(drop=True)

        write_to_parquet(df_all, path_intra)

# %% codecell
