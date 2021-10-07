"""Starter for executing yfinance options sequence."""
# %% codecell
import os
from pathlib import Path
from io import BytesIO
import pandas as pd
import requests

try:
    from scripts.dev.multiuse.api_helpers import get_sock5_nord_proxies
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg
    from scripts.dev.multiuse.help_class import df_create_bins
    from scripts.dev.data_collect.iex_class import get_options_symbols
    from scripts.dev.data_collect.yfinance_funcs import get_cboe_ref, yoptions_still_needed
except ModuleNotFoundError:
    from multiuse.api_helpers import get_sock5_nord_proxies
    from multiuse.help_class import baseDir, getDate, help_print_arg
    from multiuse.help_class import df_create_bins
    from data_collect.iex_class import get_options_symbols
    from data_collect.yfinance_get_options import execute_yahoo_options
    from data_collect.yfinance_funcs import get_cboe_ref, yoptions_still_needed

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

# %% codecell

# %% codecell


def yoptions_combine_temp_all(keep_temps=False, keep_unfin=False):
    """Combine temporary options with historical records."""
    dt = getDate.query('iex_eod')
    yr = dt.year
    path_base = Path(baseDir().path, 'derivatives/end_of_day')
    temps = list(Path(path_base, 'temp', str(yr)).glob('**/*.parquet'))

    for path in temps:
        try:
            sym = str(path.resolve()).split('_')[-1].split('.')[0]
            path_to_write = Path(path_base, str(yr), sym.lower()[0], f"_{sym}.parquet")

            if path_to_write.is_file():
                df_old = pd.read_parquet(path_to_write)
                df_new = pd.read_parquet(path)
                # Combine dataframes and write to local file
                df_all = pd.concat([df_old, df_new])
                df_all.to_parquet(path_to_write)
                # Remove temp file
                if not keep_temps:
                    os.remove(path)
            else:
                df_new = pd.read_parquet(path)
                df_new.to_parquet(path)
        except Exception as e:
            help_print_arg(str(e))

    if not keep_unfin:
        unfinished_paths = list(Path(path_base, 'unfinished').glob('*.parquet'))
        if unfinished_paths:
            for upath in unfinished_paths:
                os.remove(upath)

# %% codecell


class SetUpYahooOptions():
    """A class to run the yahoo options function, and store results."""
    sym_df, testing = False, False

    def __init__(self, followup=False, testing=False):
        self.testing = testing
        proxies = get_sock5_nord_proxies()

        if followup:
            self.sym_df = yoptions_still_needed()
        else:
            self.sym_df = get_cboe_ref(ymaster=True)

        df_comb = self.get_bins_and_combine(self, proxies)

        self.initiate_for_loop(self, df_comb)

    def get_bins_and_combine(cls, self, proxies):
        """Create bins, merge back to og df."""
        bin_size = int(self.sym_df.shape[0] / (len(proxies) - 1))
        df_syms = df_create_bins(self.sym_df, bin_size=bin_size)
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
            if self.testing:
                help_print_arg(str(arg))
            try:
                from app.tasks import execute_func
                kwargs = {'df': arg.to_json()}
                execute_func.delay('execute_yoptions', **kwargs)
            except ModuleNotFoundError:
                execute_yahoo_options(arg.to_json())
                help_print_arg('Execute yahoo options not found')
