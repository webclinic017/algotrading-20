"""Apca Calendar."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import write_to_parquet

# %% codecell


class ApcaCalendar():
    """Alpaca calendar for market days."""

    def __init__(self, resp, fpath, **kwargs):
        self.df = self._convert_to_df(self, resp, **kwargs)
        self._write_to_parquet(self, self.df, fpath, **kwargs)

    @classmethod
    def _convert_to_df(cls, self, resp, **kwargs):
        """Convert repsonse object to json, then to dataframe."""
        df = pd.json_normalize(resp.json())
        df['date'] = pd.to_datetime(df['date'])
        return df

    @classmethod
    def _write_to_parquet(cls, self, df, fpath, **kwargs):
        """Write to dataframe."""
        write_to_parquet(df, fpath, **kwargs)
