"""Master function for hist prices."""
# %% codecell
###############################################################


import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, df_create_bins, help_print_arg
    from scripts.dev.multiuse.api_helpers import rate_limit
    from scripts.dev.data_collect.hist_prices import HistPricesV2
    from scripts.dev.data_collect.apca_routines import ApcaHist
    # from app.tasks import execute_func  - imported in body of class
    # from app.tasks_test import print_arg_test
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, df_create_bins, help_print_arg
    from multiuse.api_helpers import rate_limit
    from data_collect.hist_prices import HistPricesV2
    from data_collect.apca_routines import ApcaHist

# %% codecell
###############################################################


class SplitGetHistPrices():
    """Master class to split cs/otc stocks and get data."""

    # Normal is the non-otc data available through IEX cloud
    # OTC is over the counter
    # Apca is to get all historical stock data available through alpaca API
    # Struct is structured products, warrants

    def __init__(self, testing=False, remote=True, normal=False, otc=False, apca=False, warrants=False, last_month=False):
        self.determine_params(self, testing, normal, otc, apca, warrants)
        bins_unique = self.create_df_bins(self)
        result = False
        self.last_month = last_month

        if apca:
            result = self.apca_get_data(self, testing)
        elif remote and not apca:
            result = self.remote_get_data(self, bins_unique, testing)
        else:
            result = self.local_get_data(self, bins_unique, testing)

        if result:
            self.call_combined(self, normal, otc, apca)

    @classmethod
    def determine_params(cls, self, testing, normal, otc, apca, warrants):
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
        elif warrants:
            fpath = f"{base_fpath}/all_symbols.gz"
            df = pd.read_json(fpath, compression='gzip')
            wt_df = df[df['type'].isin(['wt', 'ut', 'rt'])]
            self.df = wt_df.copy(deep=True)

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

        if testing:
            kwargs['sym_list'] = self.df['symbol'].sample(n=10).tolist()
            help_print_arg(kwargs)
            rate_limit(ApcaHist, testing=True, **kwargs)
        else:
            rate_limit(ApcaHist, testing=False, **kwargs)

        return True

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
            # Check if getting data for the last month
            if self.last_month:
                kwargs['last_month'] = True
            # Call tasks.execute_func to get data for each sym_list (bin int)
            execute_func.delay('hist_prices_sub', **kwargs)

        return True

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
            if self.last_month:
                for sym in sym_list:
                    HistPricesV2(sym, last_month=True)
            else:
                for sym in sym_list:
                    HistPricesV2(sym)

        return True

    @classmethod
    def call_combined(cls, self, normal, otc, apca):
        """Combine all apca data."""
        try:
            from app.tasks import execute_func
            if normal or otc:
                execute_func.delay('combine_daily_stock_eod')
            elif apca:
                execute_func.delay('combine_apca_stock_eod')
        except ModuleNotFoundError:
            pass
