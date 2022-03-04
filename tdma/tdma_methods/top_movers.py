"""TDMA method for top movers in each of the 3 funds."""
# %% codecell
from pathlib import Path
from datetime import datetime
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet

# %% codecell


class TdmaSectorMovers():
    """Class for sector movers."""

    def __init__(self, api_val, resp, **kwargs):
        self.df = self._tdma_sector_movers(self, resp, **kwargs)

    @classmethod
    def _tdma_sector_movers(cls, self, resp, **kwargs):
        """Get and record tdma top movers every minute."""
        # Pos sector values: # COMPX DJI SPX.X
        url = resp.url
        sector = url.split('/')[-2]

        df = pd.json_normalize(resp.json())
        df['sector'] = sector
        df['timestamp'] = pd.Timestamp(datetime.now())

        dt = getDate.query('mkt_open')
        bpath = Path(baseDir().path, 'tickers', 'movers')
        fpath = bpath.joinpath(f"_{dt}.parquet")
        write_to_parquet(df, fpath, combine=True)

        return df
