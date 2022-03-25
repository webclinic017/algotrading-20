"""Last watchlist performance (this current week)."""
# %% codecell

import numpy as np
import pandas as pd

try:
    from scripts.dev.twitter.watchlists.possible_trades import TwitterPossibleTrades
    from scripts.dev.multiuse.help_class import getDate, help_print_arg
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from twitter.watchlists.possible_trades import TwitterPossibleTrades
    from multiuse.help_class import getDate, help_print_arg
    from api import serverAPI

# %% codecell


class LastWatchlistPerformance(TwitterPossibleTrades):
    """Last week watchlist performance."""

    def __init__(self, **kwargs):
        self._get_class_vars(self, **kwargs)
        self._instantiate_pos_trades(self, **kwargs)
        self.df_watch_lw = self._get_latest_watchlist(self)
        self.df_l_sub = self._get_latest_prices(self)
        self.df_lsyms = self._merge_add_metrics(self)
        print('LastWatchlistPerformance: df accessible under self.df_lsyms')

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.days_cutoff = kwargs.get('days_cutoff', 3)

    @classmethod
    def _instantiate_pos_trades(cls, self, **kwargs):
        """Instantiate TwitterPossibleTrades class."""
        TwitterPossibleTrades.__init__(self, **kwargs)

    @classmethod
    def _get_latest_watchlist(cls, self):
        """Get latest watchlist - set by days_cutoff in kwargs."""
        df_atrades = self.df_atrades.copy()
        dt = getDate.query('iex_close')
        df_atrades['days_past'] = ((dt - df_atrades['created_at_watch']
                                   .dt.date).dt.days)

        df_watch_lw = (df_atrades[df_atrades['days_past'] < self.days_cutoff]
                       .copy()
                       .sort_values(by='created_at_watch')
                       .drop_duplicates(subset='symbol', keep='last'))

        return df_watch_lw

    @classmethod
    def _get_latest_prices(cls, self):
        """Get latest prices from IEX previous data call."""
        df_last = serverAPI('stock_close_prices').df

        last_cols = (['symbol', 'date', 'fOpen', 'fHigh',
                      'fClose', 'fLow', 'changePercent'])
        watch_syms = self.df_watch_lw['symbol'].tolist()
        df_l_sub = (df_last[
                    df_last['symbol'].isin(watch_syms)]
                    .loc[:, last_cols]
                    .copy())
        return df_l_sub

    @classmethod
    def _merge_add_metrics(cls, self):
        """Merge dataframes and add performance metrics."""
        cols_com = self.df_l_sub.columns.intersection(self.df_watch_lw.columns)
        if self.verbose:
            help_print_arg(f"Merge on {str(cols_com)}")

        df_lsyms = self.df_l_sub.merge(self.df_watch_lw, on='symbol')
        # Check for any _x|_y columns from incorrect merge
        problem_cols = (df_lsyms.columns
                        [df_lsyms.columns.
                         str.contains('_x|_y', regex=True)])
        if not problem_cols.empty:
            msg1 = 'LastWatchlistPerformance._merge_add_metrics'
            print(f"{msg1}: {str(problem_cols)}")

        # Columns to convert to floats for later analysis
        cols_to_con = ['callP', 'putP']
        df_lsyms[cols_to_con] = df_lsyms[cols_to_con].astype(np.float64)

        df_lsyms['putTrig'] = (np.where(
            df_lsyms['fLow'] < df_lsyms['putP'], True, False
        ))
        df_lsyms['callTrig'] = (np.where(
            df_lsyms['fHigh'] > df_lsyms['callP'], True, False
        ))

        df_lsyms['match'] = (np.where(
            df_lsyms[['callTrig', 'putTrig']].all(axis=1), 'both',
            np.where(
                df_lsyms['callTrig'], 'call',
                np.where(
                    df_lsyms['putTrig'], 'put', np.NaN
                )
            )
        ))

        df_lsyms['priceDiff'] = (np.where(
            df_lsyms['match'] == 'put',
            df_lsyms['putP'].sub(df_lsyms['fClose']),
            np.where(
                df_lsyms['match'] == 'call',
                df_lsyms['fClose'] - df_lsyms['callP'],
                np.where(
                    df_lsyms['match'] == 'both', 0, np.NaN
                )
            )
        ))

        return df_lsyms
