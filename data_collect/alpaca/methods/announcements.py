"""Apca Announcements."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import write_to_parquet, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import write_to_parquet, help_print_arg

# %% codecell


class ApcaAnnouncements():
    """Alpaca announcements."""
    # Limited to the last 90 days

    def __init__(self, resp, fpath, **kwargs):
        self.verbose = kwargs.get('verbose', False)
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
        write_to_parquet(df, fpath, combine=True, **kwargs)

        if self.verbose:
            help_print_arg(f"ApcaAnnouncements: fpath {str(fpath)}")



# %% codecell
