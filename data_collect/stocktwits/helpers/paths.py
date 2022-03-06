"""Stocktwits paths."""
# %% codecell
from pathlib import Path
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate

# %% codecell


class StockTwitsPaths():
    """Stocktwits paths that can be inherited."""
    bpath = Path(baseDir().path, 'social', 'stocktwits')

    def __init__(self, **kwargs):
        self.return_df = kwargs.get('return_df', False)
        self.f_msgs = self.bpath.joinpath('stream', '_user_messages.parquet')
        self.f_syms = self.bpath.joinpath('stream', '_symbols.parquet')
        self.f_trend = self._get_f_trend(self, **kwargs)

        if self.return_df:
            if self.f_msgs.exists():
                self.df_msgs = pd.read_parquet(self.f_msgs)
            if self.f_syms.exists():
                self.df_syms = pd.read_parquet(self.f_syms)
            if self.f_trend.exists():
                self.df_trend = pd.read_parquet(self.f_trend)

    @classmethod
    def _get_f_trend(cls, self, **kwargs):
        """Get f trend path, assume current year."""
        dt = kwargs.get('dt', getDate.query('iex_eod'))
        return self.bpath.joinpath('trend', f'_{str(dt.year)}.parquet')
