"""Master function for hist prices."""
# %% codecell
###############################################################


import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, df_create_bins
    from scripts.dev.multiuse.api_helpers import rate_limit
    from scripts.dev.data_collect.hist_prices import HistPricesV2
    from scripts.dev.data_collect.apca_routines import ApcaHist
    # from app.tasks import execute_func  - imported in body of class
    # from app.tasks_test import print_arg_test
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, df_create_bins
    from multiuse.api_helpers import rate_limit
    from data_collect.hist_prices import HistPricesV2
    from data_collect.apca_routines import ApcaHist

# %% codecell
###############################################################


class SplitGetHistPrices():
    """Master class to split cs/otc stocks and get data."""

    def __init__(self, testing=False, remote=True, normal=False, otc=False, apca=False):
        self.determine_params(self, testing, normal, otc, apca)
        bins_unique = self.create_df_bins(self)

        if apca:
            self.apca_get_data(self, testing)
        elif remote and not apca:
            self.remote_get_data(self, bins_unique, testing)
        else:
            self.local_get_data(self, bins_unique, testing)

    @classmethod
    def determine_params(cls, self, testing, normal, otc, apca):
        """Determine fpath and other params."""
        base_fpath = f"{baseDir().path}/tickers"
        fpath = ''
        # If using iex data, read compressed iex ref files
        if normal or otc:
            if normal:
                fpath = f"{base_fpath}/all_symbols.gz"
            elif otc:
                fpath = f"{base_fpath}/otc_syms.gz"
            # Read compressed symbol file
            self.df = pd.read_json(fpath, compression='gzip')
        # If using alpaca ref file, get only active data symbols
        elif apca:
            fpath = f"{base_fpath}/apca_ref.gz"
            df = pd.read_json(fpath, compression='gzip')
            self.df = df[df['status'] == 'active'].copy(deep=True)

    @classmethod
    def create_df_bins(cls, self):
        """Create dataframe bins with default 1000 chunk size."""
        self.df = df_create_bins(self.df)
        # Get list of unique symbols
        bins_unique = self.df['bins'].unique().tolist()

        return bins_unique

    @classmethod
    def apca_get_data(cls, self, testing):
        """Start long running apca historical data request."""
        kwargs = {'sym_list': self.df['symbol'].tolist()}
        rate_limit(ApcaHist, testing=True, **kwargs)

    @classmethod
    def remote_get_data(cls, self, bins_unique, testing):
        """Call tasks.execute_func for subdivided remote server processes."""
        try:
            from app.tasks import execute_func
        except ModuleNotFoundError:
            pass
        # For each of the 1000 symbol bins, get data
        for bin in bins_unique:
            syms_part = self.df[self.df['bins'] == bin]
            if testing:
                syms_part = syms_part.sample(n=5).copy(deep=True)
            sym_list = syms_part['symbol'].tolist()
            # Define **kwargs to unpack in execute_func, for each bin
            kwargs = {'sym_list': sym_list}
            # Call tasks.execute_func to get data for each sym_list (bin int)
            execute_func.delay('hist_prices_sub', **kwargs)

    @classmethod
    def local_get_data(cls, self, bins_unique, testing):
        """Get historical data using imported function."""
        # For each of the 1000 symbol bins, get data
        for bin in bins_unique:
            syms_part = self.df[self.df['bins'] == bin]
            if testing:
                syms_part = syms_part.sample(n=5).copy(deep=True)
            sym_list = syms_part['symbol'].tolist()
            # Using list of symbols, call function to get data and store local
            for sym in sym_list:
                HistPricesV2(sym)
