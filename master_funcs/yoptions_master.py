"""Starter for executing yfinance options sequence."""
# %% codecell
import os
from pathlib import Path
import pandas as pd

try:
    from scripts.dev.multiuse.api_helpers import get_sock5_nord_proxies
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg
    from scripts.dev.multiuse.help_class import df_create_bins
    from scripts.dev.data_collect.iex_class import get_options_symbols
except ModuleNotFoundError:
    from multiuse.api_helpers import get_sock5_nord_proxies
    from multiuse.help_class import baseDir, getDate, help_print_arg
    from multiuse.help_class import df_create_bins
    from data_collect.iex_class import get_options_symbols

# %% codecell


def collect_rest_of_yoptions():
    """After a period of time, collect rest of data."""
    # Follow up to the first sequence of requests
    path = Path(baseDir().path, 'derivatives/end_of_day/unfinished')
    paths = list(path.glob('*.parquet'))

    for fpath in paths:
        df = pd.read_parquet(fpath)
        if df.empty:
            os.remove(fpath)
        else:
            try:
                from app.tasks import execute_func
                kwargs = {'df': df.to_json()}
                execute_func.delay('execute_yoptions', **kwargs)
            except ModuleNotFoundError:
                help_print_arg('Execute yahoo options not found')


class SetUpYahooOptions():
    """A class to run the yahoo options function, and store results."""

    def __init__(self):
        proxies = get_sock5_nord_proxies()
        sym_df = get_options_symbols()

        df_comb = self.get_bins_and_combine(self, proxies, sym_df)

        self.initiate_for_loop(self, df_comb)

    def get_bins_and_combine(cls, self, proxies, sym_df):
        """Create bins, merge back to og df."""
        bin_size = int(sym_df.shape[0] / (len(proxies) - 1))
        df_syms = df_create_bins(sym_df, bin_size=bin_size)
        bins = df_syms['bins'].unique().to_numpy()

        bin_df = pd.DataFrame(columns=['bins', 'proxy'])
        for bin, row in zip(bins, proxies):
            bin_df = bin_df.append({'bins': bin, 'proxy': row}, ignore_index=True)

        df_comb = pd.merge(df_syms, bin_df, on='bins')
        self.bins = bins

        return df_comb

        # args = [df_comb[df_comb['bins'] == n] for n in iter(self.bins)]
    def initiate_for_loop(cls, self, df_comb):
        """Initiate for loop sequence."""
        args = [df_comb[df_comb['bins'] == n] for n in iter(self.bins)]
        for arg in args:
            try:
                from app.tasks import execute_func
                kwargs = {'df': arg.to_json()}
                execute_func.delay('execute_yoptions', **kwargs)
            except ModuleNotFoundError:
                help_print_arg('Execute yahoo options not found')
