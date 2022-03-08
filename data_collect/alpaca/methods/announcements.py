"""Apca Announcements."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import write_to_parquet

# %% codecell


class ApcaAnnouncements():
    """Alpaca announcements."""

    def __init__(self, resp, fpath, **kwargs):
        self.df = self._convert_to_df(self, resp, **kwargs)
        self._write_to_parquet(self, self.df, fpath, **kwargs)

    @classmethod
    def _convert_to_df(cls, self, resp, **kwargs):
        """Convert repsonse object to json, then to dataframe."""
        df = pd.json_normalize(resp.json())
        return df

    @classmethod
    def _write_to_parquet(cls, self, df, fpath, **kwargs):
        """Write to dataframe and drop on id."""
        kwargs = {'cols_to_drop': ['id']}
        write_to_parquet(df, fpath, **kwargs)

# %% codecell
