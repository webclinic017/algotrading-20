"""TDMA Helpers."""
# %% codecell
from pathlib import Path

import pandas as pd
try:
    from scripts.dev.multiuse.help_class import getDate, baseDir
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir

# %% codecell


class TDMA_Paths():
    """Helper Functions for TDMA."""

    def get_fpath(method, **kwargs):
        """Get fpath for specified method and symbol."""
        return TDMA_Paths.find_fpath(TDMA_Paths, method, **kwargs)

    @staticmethod
    def find_fpath(self, method, **kwargs):
        """Find fpath but with self inheritance."""
        symbol = kwargs.get('symbol', False)
        return_df = kwargs.get('return_df', False)
        verbose = kwargs.get('verbose', False)

        if method == 'options_chain' and symbol:
            fpath = self.option_fpath(symbol, **kwargs)
            if return_df:
                if fpath.exists():
                    return pd.read_parquet(fpath)
                else:
                    return fpath
            else:
                return fpath

    @staticmethod
    def option_fpath(self, symbol, **kwargs):
        """Get fpath for options."""
        # Assume series instead of summary
        bpath = Path(baseDir().path, 'derivatives', 'tdma', 'series')
        # Assume current year
        dt = getDate.query('iex_eod')

        sym1 = symbol.lower()[0]
        sym2 = f"_{symbol}.parquet"
        sympath = Path(str(dt.year), sym1, sym2)
        fpath = bpath.joinpath(sympath)
        return fpath
        # fpath_sum = bpath.joinpath('summary', sympath)


# %% codecell
