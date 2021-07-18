"""Master function for hist prices."""
# %% codecell
###############################################################


import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, df_create_bins
    from scripts.dev.data_collect.hist_prices import HistPricesV2
    from ..tasks import execute_func
    # from app.tasks_test import print_arg_test
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, df_create_bins
    from data_collect.hist_prices import HistPricesV2

# %% codecell
###############################################################


class SplitGetHistPrices():
    """Master class to split cs/otc stocks and get data."""

    def __init__(self, testing=False, remote=True, normal=False, otc=False):
        self.determine_params(self, testing, normal, otc)
        bins_unique = self.create_df_bins(self)

        if testing:
            self.testing_get_data(self)
        elif remote:
            self.remote_get_data(self, bins_unique)
        else:
            self.local_get_data(self, bins_unique)

    @classmethod
    def determine_params(cls, self, testing, normal, otc):
        """Determine fpath and other params."""
        fpath = ''

        if normal:
            fpath = f"{baseDir().path}/tickers/all_symbols.gz"
        elif otc:
            fpath = f"{baseDir().path}/tickers/otc_syms.gz"

        self.df = pd.read_json(fpath, compression='gzip')

    @classmethod
    def create_df_bins(cls, self):
        """Create dataframe bins with default 1000 chunk size."""
        self.df = df_create_bins(self.df)
        # Get list of unique symbols
        bins_unique = self.df['bins'].unique().tolist()

        return bins_unique

    @classmethod
    def testing_get_data(cls, self):
        """Testing for 50 otc symbols at random to see if this works."""
        sample_df = self.df.sample(n=50)
        # Convert symbol column to list
        syms_list = sample_df['symbol'].tolist()

        for sym in syms_list:
            HistPricesV2(sym)

    @classmethod
    def remote_get_data(cls, self, bins_unique):
        """Call tasks.execute_func for subdivided remote server processes."""
        # For each of the 1000 symbol bins, get data
        for bin in bins_unique:
            syms_part = self.df[self.df['bins'] == bin]
            sym_list = syms_part['symbol'].tolist()
            # Define **kwargs to unpack in execute_func, for each bin
            kwargs = {'sym_list': sym_list}
            # Call tasks.execute_func to get data for each sym_list (bin int)
            execute_func.delay('hist_prices_sub', **kwargs)

    @classmethod
    def local_get_data(cls, self, bins_unique):
        """Get historical data using imported function."""
        # For each of the 1000 symbol bins, get data
        for bin in bins_unique:
            syms_part = self.df[self.df['bins'] == bin]
            sym_list = syms_part['symbol'].tolist()
            # Using list of symbols, call function to get data and store local
            for sym in sym_list:
                HistPricesV2(sym)
