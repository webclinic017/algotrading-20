"""Apca local fpaths."""
# %% codecell
from pathlib import Path

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir

# %% codecell


class ApcaPaths():
    """Dict of local fpaths."""
    bdir = Path(baseDir().path, 'alpaca')

    def __init__(self, api_val, **kwargs):
        self._unpack_kwargs(self, **kwargs)
        self.fdict = self._make_path_dict(self, self.bdir, **kwargs)
        self.fpath = self._get_fpath(self, api_val, **kwargs)
        if self.return_df:
            self.df = self._get_df(self, **kwargs)

    @classmethod
    def _unpack_kwargs(cls, self, **kwargs):
        """Unpack kwargs and assign to class variables."""
        self.verbose = kwargs.get('verbose', None)
        self.return_df = kwargs.get('return_df', None)
        # Possible date passed
        self.dt = kwargs.get('dt', getDate.query('iex_eod'))
        self.year = str(self.dt.year)

    @classmethod
    def _make_path_dict(cls, self, bdir, **kwargs):
        """Make path dict and return as fdict."""
        fdict = {
            'news_realtime': (bdir.joinpath('news', 'real_time',
                              f"_{self.year}.parquet")),
            'news_historical': (bdir.joinpath('news', 'historical',
                                f"_{self.year}.parquet")),
            'mergers':   (bdir.joinpath('corporate_actions',
                          "_mergers.parquet")),
            'dividends': (bdir.joinpath('corporate_actions',
                          "_dividends.parquet")),
            'splits':   (bdir.joinpath('corporate_actions',
                         "_splits.parquet")),
            'spinoffs':   (bdir.joinpath('corporate_actions',
                           "_spinoffs.parquet")),
            'calendar': (bdir.joinpath('calendar', '_market_days.parquet'))
        }

        return fdict

    @classmethod
    def _get_fpath(cls, self, api_val, **kwargs):
        """Get fpath from fdict."""
        fpath = self.fdict.get(api_val, None)
        sub_method = kwargs.get('params', {}).get('ca_types')
        if isinstance(sub_method, list):
            sub_method = sub_method[0].lower() + 's'  # Get the first value
            fpath = self.fdict.get(sub_method, None)

        return fpath

    @classmethod
    def _get_df(cls, self, **kwargs):
        """Get and return df if it exists else return None."""
        if self.fpath.exists():
            df = pd.read_parquet(self.fpath)
        else:
            df = None

        return df


# %% codecell
