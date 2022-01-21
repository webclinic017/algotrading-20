"""Iex intra-day."""
# %% codecell
from pathlib import Path
import os

import pandas as pd
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.data_collect.iex_class import urlData
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from data_collect.iex_class import urlData
    from api import serverAPI

# %% codecell


class GetIntradayIexData():
    """Get intraday (1 minute) data from IEX."""

    """
    self.syms : list of all symbols used
    self.error_dict
    self.df_errors
    """

    def __init__(self, sym_list=[], dt=False, verbose=False, ntests=False):
        if not sym_list:
            self._get_syms(self)
        else:
            self.syms = sym_list

        self._start_for_loop(self, dt, verbose, ntests)

    @classmethod
    def _get_syms(cls, self):
        """Get symbol list for which to retrieve data."""
        all_syms = serverAPI('all_symbols').df
        syms = all_syms['symbol'].unique().tolist()

        self.syms = syms

    @classmethod
    def _start_for_loop(cls, self, dt, verbose, ntests):
        """Start for loop for syms."""
        if not dt:
            dt = getDate.query('iex_eod')

        bpath = Path(baseDir().path, 'intraday', 'minute_1', str(dt.year))

        error_dict = {}
        n = 0
        # Check if ntests is a integer (if we're testing)
        if not isinstance(ntests, int):
            ntests = 5

        for sym in tqdm(self.syms):
            try:
                self._get_sym_min_data(self, sym, dt, bpath, verbose)
            except Exception as e:
                error_dict[sym] = ({'symbol': sym,
                                    'type': str(type(e)),
                                    'reason': str(e)})
                if verbose:
                    msg = f"{sym} get_sym_min_data. Reason: {str(e)}"
                    help_print_arg(msg)
            if ntests:  # If testing, eventually break
                n += 1
                if n > ntests:
                    break

        self._error_handling(self, error_dict, bpath)

    @classmethod
    def _get_sym_min_data(cls, self, sym, dt, bpath, verbose=False):
        """Get minute data for symbol. Write to file."""
        if not dt:
            dt = getDate.query('iex_eod')

        # Construct fpath
        fpath = bpath.joinpath(sym[0].lower(), f"_{sym}.parquet")
        # Construct url
        url_p1 = f"/stock/{sym.lower()}/chart/date/"
        url_p2 = f"{dt.strftime('%Y%m%d')}?chartByDay=false"
        url = f"{url_p1}{url_p2}"

        if verbose:  # If verbose print out key vars
            msg = f"Sym: {sym}, Date: {str(dt)}, fpath: {str(fpath)}, url: {url}"
            help_print_arg(msg)

        # Get data with requested url
        df_ud = urlData(url).df
        df_ud['dtime'] = (pd.to_datetime(df_ud['date'] + df_ud['minute'],
                          format='%Y-%m-%d%H:%M'))
        df_ud['date'] = pd.to_datetime(df_ud['date'], format='%Y-%m-%d')
        df_ud['symbol'] = sym
        # Write to parquet and exit function
        write_to_parquet(df_ud, fpath, combine=True)

    @classmethod
    def _error_handling(cls, self, error_dict, bpath):
        """Error handling for 1 minute data."""
        df_errors = pd.DataFrame.from_dict(error_dict)

        self.df_errors = df_errors
        self.error_dict = error_dict

        bpath_e = bpath.parent.parent
        fpath_errors = bpath_e.joinpath('errors', "_minute_1_errors.parquet")

        write_to_parquet(df_errors, fpath_errors, combine=True)

# %% codecell
