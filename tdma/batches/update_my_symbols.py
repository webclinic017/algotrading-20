"""Update my_syms local parquet file."""
# %% codecell


from pathlib import Path

import pandas as pd

try:
    from scripts.dev.tdma.tdma_api.td_api import TD_API
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet
except ModuleNotFoundError:
    from tdma.tdma_api.td_api import TD_API
    from multiuse.help_class import baseDir, write_to_parquet

# %% codecell


class TdmaWatchPositionsSymbols():
    """Get watchlists + open position symbols."""

    def __init__(self, **kwargs):
        self._twps_class_vars(self, **kwargs)
        self.df_cp = self._twps_watchlist_symbols(self)
        self.df_pos = self._twps_get_account_positions(self)
        self.df_my_syms = self._twps_combine_symbols(self)
        self._twps_write_to_parquet(self)

    @classmethod
    def _twps_class_vars(cls, self, **kwargs):
        """Get class variables, unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        # Define fpath for my symbols
        self.f_me = Path(baseDir().path, 'tickers', 'my_syms.parquet')

    @classmethod
    def _twps_watchlist_symbols(cls, self, **kwargs):
        """Get watchlist symbols (current positions = mobile watchlist)."""
        # Get watchlists from TD Ameritrade Account
        tm_watch = TD_API(api_val='get_watchlists')
        df_w = (pd.json_normalize(
                tm_watch.resp.json(),
                meta=['name', 'watchlistId', 'accountId'],
                record_path=['watchlistItems']))
        # Keep only rows from TD Ameritrade mobile app
        df_cp = df_w[df_w['name'] == 'Current positions'].copy()

        return df_cp

    @classmethod
    def _twps_get_account_positions(cls, self, **kwargs):
        """Get open psoitions for my acccount."""
        tm_acc = TD_API(api_val='get_accounts')
        df_pos = tm_acc.df_pos.copy()

        return df_pos

    @classmethod
    def _twps_combine_symbols(cls, self):
        """Combine symbols from watchlist + open positions for 1 sym list."""
        watch_syms = self.df_cp['instrument.symbol'].tolist()
        pos_syms = self.df_pos['instrument.underlyingSymbol'].tolist()
        all_watch_syms = list(set(watch_syms + pos_syms))

        all_syms = all_watch_syms
        # If local parquet file of my symbols exists
        if self.f_me.exists():
            df_me = pd.read_parquet(self.f_me)
            # Redefining all_syms here
            all_syms = list(set(all_watch_syms + df_me['symbol'].tolist()))

        # Create new dataframe of my symbols
        df_my_syms = pd.DataFrame()
        df_my_syms['symbol'] = all_syms

        if self.verbose:
            print("""TWPS: new sym list available under
                     TdmaWatchPositionsSymbols.df_my_syms""")

        return df_my_syms

    @classmethod
    def _twps_write_to_parquet(cls, self):
        """Write to parquet for df_my_syms and self.f_me."""
        write_to_parquet(self.df_my_syms, self.f_me)
