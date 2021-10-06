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
except ModuleNotFoundError:
    from multiuse.api_helpers import get_sock5_nord_proxies
    from multiuse.help_class import baseDir, getDate, help_print_arg
    from multiuse.help_class import df_create_bins
    from data_collect.iex_class import get_options_symbols
    from data_collect.yfinance_get_options import execute_yahoo_options

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


def get_cboe_ref():
    """Get cboe reference data for use on yfinance."""
    df = None
    path_suf = f"symref_{getDate.query('cboe')}.parquet"
    path = Path(baseDir().path, 'derivatives/cboe_symref', path_suf)
    if path.is_file():
        df = pd.read_parquet(path)
    else:
        url_p1 = 'https://www.cboe.com/us/options/market_statistics/'
        url_p2 = 'symbol_reference/?mkt=cone&listed=1&unit=1&closing=1'
        url = f"{url_p1}{url_p2}"
        get = requests.get(url)

        df = pd.read_csv(BytesIO(get.content), low_memory=False)
        df.to_parquet(path)

    cols_to_drop = ['Cboe Symbol', 'Closing Only']
    df = (df.rename(columns={'Underlying': 'symbol'})
            .drop(columns=cols_to_drop))

    return df

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


def yoptions_still_needed(recreate=False):
    """Return a list of all syms:exp_dates that are missing."""
    ref_path = Path(baseDir().path, 'ref_data', 'syms_with_options.parquet')
    ref_df = pd.read_parquet(ref_path)

    path_for_temp = Path(baseDir().path, 'derivatives/end_of_day/temp/2021')
    paths_for_temp = list(path_for_temp.glob('**/*.parquet'))

    df_list = []
    for fpath in paths_for_temp:
        df_list.append(pd.read_parquet(fpath))

    df_all = pd.concat(df_list)
    df_collected = (df_all.groupby(by=['symbol'])['expDate']
                          .agg({lambda x: list(x)})
                          .reset_index()
                          .rename(columns={'<lambda>': 'expDatesStored'})
                          .copy())

    df_comb = pd.merge(ref_df, df_collected, how='left', on=['symbol'], indicator=True)
    df_left = df_comb[df_comb['_merge'] == 'left_only'].copy()

    # df_comb['expDatesNeeded'] = df_comb.apply(lambda row: list(set(row.expDates) - set(row.expDatesStored)), axis=1)

    # if recreate:
    #    df_comb = df_comb.drop(columns=['expDates', 'expDatesStored'])

    return df_left


class SetUpYahooOptions():
    """A class to run the yahoo options function, and store results."""
    sym_df, testing = False, False

    def __init__(self, followup=False, testing=False):
        self.testing = testing
        proxies = get_sock5_nord_proxies()

        if followup:
            self.sym_df = yoptions_still_needed()
        else:
            self.sym_df = get_cboe_ref()

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
