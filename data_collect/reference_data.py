"""Reference data collection."""
# %% codecell
from pathlib import Path
from time import sleep

import pandas as pd


# %% codecell

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg, help_print_error, write_to_parquet
    from scripts.dev.data_collect.iex_routines import urlData
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg, help_print_error, write_to_parquet
    from data_collect.iex_routines import urlData


# %% codecell

class IntSyms():
    """Get international symbols ref data from IEX."""
    """
    Exchange df: self.exch_df
    All international symbols: self.all_int_syms
    """

    def __init__(self):

        self.get_int_exchanges(self)
        self.get_all_int_syms(self)
        self.int_syms_to_parquet(self)

    @classmethod
    def get_int_exchanges(cls, self):
        """Get dataframe of all international exchanges."""
        path = Path(baseDir().path, 'ref_data/symbol_ref', 'int_exch.parquet')

        if path.exists():
            # Read local file and assign to class attribute
            self.exch_df = pd.read_parquet(path)
        else:
            int_exch_url = '/ref-data/exchanges'
            int_exch_syms = urlData(int_exch_url).df
            # Write to local parquet file
            write_to_parquet(int_exch_syms, path)
            self.exch_df = int_exch_syms

    @classmethod
    def get_all_int_syms(cls, self):
        """Loop through exchanges and get all international symbols."""
        df_list = []

        for exch in self.exch_df['exchange'].tolist():
            url = f"/ref-data/exchange/{exch}/symbols"
            try:
                ud = urlData(url)
                df = ud.df.copy()
                df_list.append(df)
                sleep(.5)
            except Exception as e:  # If error, print error and exchange to console
                msg_1 = f"IntSyms Error getting data for exchange {exch}"
                msg_2 = f" with error type: {type(e)} and error: {str(e)}"
                msg_3 = f"Url: {url} get.status_code: {ud.get.status_code} message: {ud.get.text}"
                help_print_arg(f"{msg_1}{msg_2}")
                help_print_arg(msg_3)

        df_all = pd.concat(df_list).reset_index(drop=True)
        self.all_int_syms = df_all

    @classmethod
    def int_syms_to_parquet(cls, self):
        """Write international symbols to local parquet file."""
        fpath_suf = 'all_international_symbols.parquet'
        path = Path(baseDir().path, 'ref_data/symbol_ref', fpath_suf)

        write_to_parquet(self.all_int_syms, path)


# %% codecell

class IexRefData():
    """Iex reference data."""

    """
    self.bpath : base path for data storage
    self.url_dict : url dictionary for ref endpoints
    self.path_dict : fpath dictionary for ref endpoints
    """

    def __init__(self, which):
        self.make_vars(self, which)
        self.make_url_dict(self)
        self.make_path_dict(self, self.bpath)
        self.start_get_data(self)

    @classmethod
    def make_vars(cls, self, which):
        """Make base path."""
        bpath = Path(baseDir().path, 'ref_data/fresh')
        self.bpath = bpath
        self.which = which

    @classmethod
    def make_url_dict(cls, self):
        """Make url dict and store as class variable."""
        url_dict = ({'iex_intra_prices': '/ref-data/symbols',
                     'iex_trading_syms': '/ref-data/iex/symbols',
                     'iex_mutual_fund': '/ref-data/mutual-funds/symbols',
                     'iex_otc_syms': '/ref-data/otc/symbols',
                     'us_exchanges': '/ref-data/market/us/exchanges',
                     'intl_exchanges': '/ref-data/exchanges',
                     'iex_tags': '/ref-data/tags'})
        self.url_dict = url_dict

    @classmethod
    def make_path_dict(cls, self, bpath):
        """Make local path dictionary."""
        path_dict = {}
        # Iterate through url_dict.keys
        for key in self.url_dict.keys():
            path_dict[key] = str(Path(bpath, f"{key}.parquet"))

        self.path_dict = path_dict

    @classmethod
    def start_get_data(cls, self):
        """Get data with which param."""

        if self.which == 'all':
            for key in self.url_dict.keys():
                self._get_data(self, key)
        else:  # If only trying to get one url
            self._get_data(self, self.which)

    @classmethod
    def _get_data(cls, self, which):
        """Get data for individual url."""
        try:
            ud = urlData(self.url_dict[which])
            if isinstance(ud.df, pd.DataFrame):
                self._write_to_parquet(self, ud.df, which)
        except Exception as e:
            help_print_error(e, parent='IexRefData', ud=ud)

    @classmethod
    def _write_to_parquet(cls, self, df, which=False):
        """Write response to parquet."""
        path = None

        if which:
            path = self.path_dict[which]
        else:
            path = self.path_dict[self.which]

        write_to_parquet(df, path)


# %% codecell
