"""Correlation plot for closest peers."""
# %% codecell
from pathlib import Path
from datetime import date

import pandas as pd
from tqdm import tqdm

try:
    from scripts.dev.multiuse.pd_funcs import mask
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet
    from scripts.dev.workbooks_fib.fib_funcs import read_clean_combined_all
except ModuleNotFoundError:
    from multiuse.pd_funcs import mask
    from multiuse.help_class import baseDir, write_to_parquet
    from workbooks_fib.fib_funcs import read_clean_combined_all


# %% codecell
pd.DataFrame.mask = mask

# %% codecell


class PeerList():
    """Calculate corr of returns."""

    """
    ~ 30 seconds to run
    var : self.fpath : ref_data/peer_list/_peers.parquet
    var : self.df_corr : dataframe for correlation of stock returns
    var : self.df_sym_all : all extreme +/- .80 corrs
    var : self.df : final df
    """

    def __init__(self, filter_extremes=False):
        self._construct_params(self, filter_extremes)
        self._get_create_corr_df(self, filter_extremes)
        self._clean_up_and_write(self, self.df_corr)

    @classmethod
    def _construct_params(cls, self, filter_extremes):
        """Construct parameters used for class."""
        bpath = Path(baseDir().path, 'ref_data', 'peer_list')
        if filter_extremes:
            self.fpath = bpath.joinpath('_peers_extreme.parquet')
        else:
            self.fpath = bpath.joinpath('_peers.parquet')

    @classmethod
    def _get_create_corr_df(cls, self, filter_extremes):
        """Get eod of day stock data, convert to corr dataframe."""
        dt = date(2021, 1, 1)
        # dt = None
        df_all = read_clean_combined_all(dt=dt)

        cols_to_keep = ['symbol', 'date', 'fChangeP']
        df_mod = df_all[cols_to_keep].copy()

        if filter_extremes:
            cond1 = (df_mod['fChangeP'] > .05)
            cond2 = (df_mod['fChangeP'] < -.05)
            df_mod = df_mod[cond1 | cond2].copy()

        df_piv = (df_mod.pivot(index='date',
                  columns='symbol', values='fChangeP'))

        self.df_corr = df_piv.corr()

    @classmethod
    def _clean_up_and_write(cls, self, df_sym_all):
        """Clean data and write to local fpath."""
        df_sym_all.index.name = 'key'
        df_sym_all = df_sym_all.stack().to_frame()
        df_sym_all.rename(columns={0: 'corr'}, inplace=True)
        df_key_all = df_sym_all[~df_sym_all['corr'].isin([1, -1])]

        self.df = df_key_all.reset_index()

        write_to_parquet(df_key_all, self.fpath)


# %% codecell
