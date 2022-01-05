"""Starter for executing yfinance options sequence."""
# %% codecell
import os
from pathlib import Path
from io import BytesIO
import pandas as pd
import requests

try:
    from scripts.dev.multiuse.api_helpers import get_sock5_nord_proxies
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg, df_create_bins, write_to_parquet
    from scripts.dev.multiuse.symbol_ref_funcs import get_all_symbol_ref
    from scripts.dev.data_collect.iex_class import get_options_symbols
    from scripts.dev.data_collect.yfinance_funcs import get_cboe_ref, yoptions_still_needed, get_yoptions_unfin
    from scripts.dev.data_collect.yfinance_info_funcs import execute_yahoo_func
except ModuleNotFoundError:
    from multiuse.api_helpers import get_sock5_nord_proxies
    from multiuse.help_class import baseDir, getDate, help_print_arg, df_create_bins, write_to_parquet
    from multiuse.symbol_ref_funcs import get_all_symbol_ref
    from data_collect.iex_class import get_options_symbols
    from data_collect.yfinance_get_options import execute_yahoo_options
    from data_collect.yfinance_funcs import get_cboe_ref, yoptions_still_needed, get_yoptions_unfin
    from data_collect.yfinance_info_funcs import execute_yahoo_func

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


def yoptions_combine_temp_all(keep_temps=False, keep_unfin=False, verbose=False):
    """Combine temporary options with historical records."""
    dt = getDate.query('iex_eod')
    yr = dt.year
    path_base = Path(baseDir().path, 'derivatives/end_of_day')
    temps = list(Path(path_base, 'temp', str(yr)).glob('**/*.parquet'))

    for tpath in temps:
        try:
            # Derive symbol from temp fpath, construct new path to write
            sym = str(tpath.resolve()).split('_')[-1].split('.')[0]
            path_to_write = Path(path_base, str(yr), sym.lower()[0], f"_{sym}.parquet")

            if verbose:
                n_pre = "yoptions_combine_temp_all: derived symbol path is"
                help_print_arg(f"{n_pre} {sym}")
                help_print_arg(f"path_to_write: {str(path_to_write)}")
                help_print_arg(f"temp path: {str(tpath)}")

            if path_to_write.is_file():
                df_old = pd.read_parquet(path_to_write)
                df_new = pd.read_parquet(tpath)
                # Combine dataframes and write to local file
                df_all = pd.concat([df_old, df_new])
                write_to_parquet(df_all, path_to_write)

                if verbose:
                    help_print_arg(f"path_to_write for symbol {sym} exists")

                # Remove temp file
                if not keep_temps:
                    os.remove(tpath)
            else:
                df_new = pd.read_parquet(tpath)
                write_to_parquet(df_new, path_to_write)

                if verbose:
                    help_print_arg(f"path_to_write for symbol {sym} did not exist")

        except Exception as e:
            help_print_arg(str(e))

    if not keep_unfin:
        unfinished_paths = list(path_base.joinpath('unfinished').glob('*.parquet'))
        if unfinished_paths:
            for upath in unfinished_paths:
                os.remove(upath)

# %% codecell


class SetUpYahooOptions():
    """A class to run the yahoo options function, and store results."""
    sym_df, testing = False, False

    """
    self.sym_df : symbol, bins
    self.comb_df : combined df with symbol, proxy
    """

    def __init__(self, followup=False, testing=False, options=True, other=False):
        self.testing, self.options, self.other = testing, options, other
        self.proceed = True
        proxies = get_sock5_nord_proxies()

        if followup and self.options:
            # self.sym_df = yoptions_still_needed()
            self.sym_df = get_yoptions_unfin()
        elif not followup and self.options:
            self.sym_df = get_cboe_ref(ymaster=True)
            # Check if no further data needed
            if self.sym_df.empty:
                self.proceed = False
        elif not followup and other == 'yinfo':
            self.sym_df = get_all_symbol_ref()
        else:
            help_print_arg('No SetUpYahooOptions __init__ condition satisfied')

        if self.proceed:  # Default True
            comb_df = self.get_bins_and_combine(self, proxies)
            self.initiate_for_loop(self, comb_df)

    @classmethod
    def get_bins_and_combine(cls, self, proxies):
        """Create bins, merge back to og df."""
        bin_size = int(self.sym_df.shape[0] / (len(proxies) - 1))
        df_syms = df_create_bins(self.sym_df, bin_size=bin_size)
        bins = df_syms['bins'].unique().to_numpy()

        bin_df = pd.DataFrame(columns=['bins', 'proxy'])
        for bin, row in zip(bins, proxies):
            bin_df = bin_df.append({'bins': bin, 'proxy': row}, ignore_index=True)

        comb_df = pd.merge(df_syms, bin_df, on='bins')
        self.bins = bins
        self.comb_df = comb_df

        return comb_df

    @classmethod
    def initiate_for_loop(cls, self, comb_df):
        """Initiate for loop sequence."""
        args = [comb_df[comb_df['bins'] == n] for n in iter(self.bins)]
        for arg in args:
            if self.testing:
                help_print_arg(str(arg))

            if self.options:
                try:
                    from app.tasks import execute_func
                    kwargs = {'df': arg.to_json()}
                    execute_func.delay('execute_yoptions', **kwargs)
                except ModuleNotFoundError:
                    execute_yahoo_options(arg.to_json())
                    help_print_arg('Execute yahoo options not found')
            elif self.other:
                try:
                    from app.tasks import execute_func
                    kwargs = {'df': arg.to_json(), 'which': self.other}
                    execute_func.delay('execute_yahoo_func', **kwargs)
                except ModuleNotFoundError:
                    execute_yahoo_func(arg.to_json(), which=self.other)
                    help_print_arg(f'Execute yahoo {self.other} not found')
