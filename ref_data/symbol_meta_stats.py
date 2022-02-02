"""Symbol reference, meta, and stats data."""
# %% codecell
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from api import serverAPI
    from multiuse.help_class import getDate

# %% codecell


class SymbolRefMetaInfo():
    """Class functions for symbol reference data, meta."""

    """
    var : self.meta : meta data dataframe (industry, sector)
    var : self.all_symbols : ref data for symbol type, identifier code
    var : self.stats : basic financial/technical stats
    """

    def __init__(self, **kwargs):
        self.meta = self._call_stock_meta_info(self)
        self.all_symbols = self._get_all_symbols(self)
        self.stats = self._get_stats_info(self)
        self.df = self._join_dataframes(self)

    @classmethod
    def _call_stock_meta_info(cls, self):
        """Get stock sector/industry data from server."""
        meta = serverAPI('company_meta').df
        cols_to_keep = ['symbol', 'sector', 'industry']
        meta_mod = (meta[cols_to_keep]
                    .drop_duplicates(subset=['symbol'])
                    .reset_index(drop=True)
                    .set_index('symbol'))

        return meta_mod

    @classmethod
    def _get_all_symbols(cls, self):
        """Get all symbols data."""
        all_symbols = serverAPI('all_symbols').df
        syms_sub = all_symbols[['symbol', 'type']]

        all_syms = syms_sub.set_index('symbol')
        return all_syms

    @classmethod
    def _get_stats_info(cls, self):
        """Get and clean company stats info from server."""
        stats = serverAPI('stats_combined').df
        stats_dt = stats[stats['date'] == stats['date'].max()].copy()

        all_symbols = serverAPI('all_symbols').df
        all_syms = (all_symbols[['symbol', 'name']]
                    .rename(columns={'name': 'companyName'}))

        ntest = pd.merge(all_syms, stats_dt, on='companyName')

        cols_to_keep = (['symbol', 'marketcap', 'beta', 'peRatio',
                         'nextEarningsDate', 'day30ChangePercent',
                         'month6ChangePercent', 'year1ChangePercent'])

        df_stats = ntest[cols_to_keep].set_index('symbol')
        df_stats['nextEarningsDate'] = (pd.to_datetime(
                                        df_stats['nextEarningsDate']))
        df_stats['dt'] = pd.to_datetime(getDate.query('iex_eod'))
        (df_stats.insert(6, 'days_until_ER',
         getDate.get_bus_day_diff(df_stats, 'dt', 'nextEarningsDate')))

        return df_stats

    @classmethod
    def _join_dataframes(cls, self):
        """Join dataframes and store as self.df."""
        df_list = [self.all_symbols, self.meta, self.stats]
        df_joined = df_list[0].join(df_list[1:]).copy()

        return df_joined

# %% codecell
