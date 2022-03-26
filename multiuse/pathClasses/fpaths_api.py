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

    keyword_dict = {}

    def __init__(self, keyword, **kwargs):
        if 'ref_data_' in keyword:
            keyword = keyword.replace('ref_data_', '')
            self.fpath = self._ref_data_fpath(self, keyword, **kwargs)
        elif 'iex_hist_' in keyword:
            keyword = keyword.replace('iex_hist_', '')
            self.fpath = self._iex_hist_fpath(self, keyword, **kwargs)

    @classmethod
    def _ref_data_fpath(cls, self, keyword, **kwargs):
        """Get ref data fpaths."""
        gs_path = Path(baseDir().path, 'errors/gz_sizes.parquet')
        ref_dict = ({
            'get_sizes': pd.read_parquet(gs_path).to_json(orient='records'),
            'data_files_sizes': Path(baseDir().path, 'logs', 'file_sizes.txt'),
        })

        self.keyword_dict = self.keyword_dict | ref_dict
        fpath = ref_dict[keyword]
        return fpath

    @classmethod
    def _iex_hist_fpath(cls, self, keyword, **kwargs):
        """Iex historical data fpaths."""
        bdir = Path(baseDir().path, 'StockEOD/combined')

        iex_hdict = ({
            'all': get_most_recent_fpath(bdir),
            'cb_all': (get_most_recent_fpath(
                       bdir.parent.joinpath(bdir.stem + '_all')))
        })

        self.keyword_dict = self.keyword_dict | iex_hdict
        fpath = iex_hdict[keyword]
        return fpath
