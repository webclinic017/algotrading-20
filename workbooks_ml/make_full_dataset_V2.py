"""Make full dataset version 2."""
# %% codecell

from pathlib import Path
import sys
import importlib
from datetime import date

import pandas as pd
import numpy as np


try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
    from scripts.dev.workbooks_fib.fib_funcs_master import fib_master
    from scripts.dev.workbooks_fib.fib_funcs_clean_analysis import fib_all_clean_combine_write, add_fib_peaks_troughs_diffs, fib_pp_cleaned
    from scripts.dev.studies.ta_lib_studies import add_ta_lib_studies, make_emas
    from scripts.dev.pre_process_data.sec_daily_index import get_collect_prep_sec_data
    from scripts.dev.pre_process_data.pre_analyst_ratings import PreProcessAnalystRatings
    from scripts.dev.ref_data.symbol_meta_stats import SymbolRefMetaInfo
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet
    from workbooks_fib.fib_funcs_master import fib_master
    from workbooks_fib.fib_funcs_clean_analysis import fib_all_clean_combine_write, add_fib_peaks_troughs_diffs, fib_pp_cleaned
    from studies.ta_lib_studies import add_ta_lib_studies, make_emas
    from pre_process_data.sec_daily_index import get_collect_prep_sec_data
    from pre_process_data.pre_analyst_ratings import PreProcessAnalystRatings
    from ref_data.symbol_meta_stats import SymbolRefMetaInfo


# %% codecell

class MlTrainingMakeFullDataset():
    """Class for making ML data training sets."""

    bpath = Path(baseDir().path, 'ml_data', 'ml_make_full_dataset')

    def __init__(self, **kwargs):
        self._mtmfd_class_vars(self, **kwargs)
        self._mtmfd_fib_functions(self, **kwargs)
        self._mtmfd_symbol_meta(self, **kwargs)
        self._mtmfd_ta_studies(self, **kwargs)
        self._mtmfd_sec_data(self, **kwargs)
        self._mtmfd_analyst_ratings(self, **kwargs)
        self._write_to_parquet(self, **kwargs)

    @classmethod
    def _mtmfd_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        self.dt = kwargs.get('dt', date(2021, 1, 1))

    @classmethod
    def _mtmfd_fib_functions(cls, self, **kwargs):
        """Fibonacci sequence functions."""
        print()
        print('Starting fib_master')
        # Step #1 - run fib sequence
        fib_master(**{'dt': self.dt})

        # All cleaned combine write
        print()
        print('Starting fib_all_clean_combine_write')
        self.df_accw = fib_all_clean_combine_write(dt=self.dt)
        f_df_accw = self.bpath.joinpath('df_accw.parquet')
        write_to_parquet(self.df_accw, f_df_accw)

        print()
        print('Starting add_fib_peaks_troughs_diffs')
        self.df_peak_troughs = add_fib_peaks_troughs_diffs(read=False).copy()
        f_peak_troughs = self.bpath.joinpath('df_peak_troughs.parquet')
        write_to_parquet(self.df_peak_troughs, f_peak_troughs)

        print()
        print('Starting fib_pp_cleaned')
        self.df_cleaned = fib_pp_cleaned(read=False, drop=False).copy()
        f_df_cleaned = self.bpath.joinpath('df_cleaned.parquet')
        write_to_parquet(self.df_cleaned, f_df_cleaned)

    @classmethod
    def _mtmfd_symbol_meta(cls, self, **kwargs):
        """Symbol meta stats."""
        print()
        print('Starting SymbolRefMetaInfo')
        self.df_sectors = SymbolRefMetaInfo(df_all=self.df_cleaned).df_all
        f_df_sectors = self.bpath.joinpath('df_sectors.parquet')
        write_to_parquet(self.df_sectors, f_df_sectors)

    @classmethod
    def _mtmfd_ta_studies(cls, self, **kwargs):
        """Technical analysis studies."""
        print()
        print('Starting technical studies: ta lib, emas')
        df_studies = add_ta_lib_studies(self.df_sectors).copy()
        self.df_studies = make_emas(df_studies).copy()
        f_df_studies = self.bpath.joinpath('df_studies.parquet')
        write_to_parquet(self.df_studies, f_df_studies)

    @classmethod
    def _mtmfd_sec_data(cls, self, **kwargs):
        """SEC data merge with existing dataframes."""
        print()
        print('Starting get_collect_prep_sec_data')
        self.sec_df = get_collect_prep_sec_data(df=self.df_studies)
        f_sec_df = self.bpath.joinpath('sec_df.parquet')
        write_to_parquet(self.sec_df, f_sec_df)

    @classmethod
    def _mtmfd_analyst_ratings(cls, self, **kwargs):
        """Get analyst ratings and merge with dataframe."""
        print()
        print('Starting PreProcessAnalystRatings')
        self.ppa_df = PreProcessAnalystRatings(df_all=self.sec_df).df
        f_ppa_df = self.bpath.joinpath('ppa_df')
        write_to_parquet(self.ppa_df, f_ppa_df)
        print('self.ppa_df last dataframe in sequence (analyst ratings)')

    @classmethod
    def _write_to_parquet(cls, self, **kwargs):
        """Write dataframe to parquet."""
        bpath = Path(baseDir().path, 'ml_data', 'ml_training')
        fpath = bpath.joinpath('_ml_training_full.parquet')
        write_to_parquet(self.ppa_df, fpath)

        print(f'MlTrainingMakeFullDataset finished. fpath is {str(fpath)}')
