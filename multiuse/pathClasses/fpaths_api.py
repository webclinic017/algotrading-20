"""Fpaths API."""
# %% codecell
from pathlib import Path

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
except ModuleNotFoundError:
    from multiuse.help_class import baseDir
    from multiuse.path_helpers import get_most_recent_fpath

# %% codecell


class FpathsAPI():
    """Get and return fpath for each API function."""

    key_dict = {}

    def __init__(self, keyword, origin, **kwargs):
        key = f"{origin}_{keyword}"

        self._ref_data_fpath(self, **kwargs)
        self._iex_hist_fpath(self, **kwargs)
        self._ml_fpath(self, **kwargs)

        self.fpath = self.key_dict[key]

    @classmethod
    def _ref_data_fpath(cls, self, **kwargs):
        """Get ref data fpaths."""
        pre = 'ref_data'
        gs_path = Path(baseDir().path, 'errors/gz_sizes.parquet')
        ref_dict = ({
            'get_sizes': pd.read_parquet(gs_path).to_json(orient='records'),
            'data_files_sizes': Path(baseDir().path, 'logs', 'file_sizes.txt'),
        })

        pre = 'ref_data'
        ref_dict = {f"{pre}_{k}": v for k, v in ref_dict.items()}
        self.key_dict = self.key_dict | ref_dict

    @classmethod
    def _iex_hist_fpath(cls, self, **kwargs):
        """Iex historical data fpaths."""
        bdir = Path(baseDir().path, 'StockEOD/combined')

        iex_hdict = ({
            'all': get_most_recent_fpath(bdir),
            'cb_all': (get_most_recent_fpath(
                       bdir.parent.joinpath(bdir.stem + '_all')))
        })

        pre = 'iex_hist'
        iex_hdict = {f"{pre}_{k}": v for k, v in iex_hdict.items()}
        self.key_dict = self.key_dict | iex_hdict

    @classmethod
    def _ml_fpath(cls, self, **kwargs):
        """Fpaths for machine learning files."""
        bdir = Path(baseDir().path, 'ml_data', 'ml_training')

        ml_dict = ({
            'subset_ref': bdir.joinpath('_df_catkeys.parquet'),
            'subset': bdir.joinpath('_df_processed.parquet')
        })

        pre = 'ml'
        ml_dict = {f"{pre}_{k}": v for k, v in ml_dict.items()}
        self.key_dict = self.key_dict | ml_dict






# %% codecell











# %% codecell
