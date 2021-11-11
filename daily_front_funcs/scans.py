"""Classes for Vue frontend scans."""
# %% codecell
########################################
import os.path
from datetime import date, timedelta
import pandas as pd
from pandas.tseries.offsets import BusinessDay

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet

# %% codecell
########################################


class scansClass():
    """Class to implement markets scans."""

    base_dir = baseDir().path
    df, comb_path = None, None

    def __init__(self, which, by):
        self.determine_params(self, which, by)
        if not isinstance(self.df, pd.DataFrame):
            self.call_func(self, which, by)
            self.write_to_parquet(self)

    @classmethod
    def determine_params(cls, self, which, by):
        """Determine parameters used for functions."""
        dt = getDate.query('iex_close')
        dt_1, path_1 = (dt - BusinessDay(n=1)).date(), False

        fpath_dict = ({
            'vol': {
                'avg': f"{self.base_dir}/scans/top_vol/_{dt}.parquet",
                'avg_1': f"{self.base_dir}/scans/top_vol/_{dt_1}.parquet"
            }
        })

        path = fpath_dict[which][by]
        comb_base_path = f"{self.base_dir}/iex_eod_quotes/combined"
        # If path is a file, read file and return
        if os.path.isfile(path):
            self.df = pd.read_parquet(path)
        else:  # Look for iex_eod_combined local file path
            comb_path = f"{comb_base_path}/_{dt}.parquet"
            if os.path.isfile(comb_path):
                self.comb_path = comb_path
                self.path = fpath_dict[which][by]
            else:  # If comb_fpath data isn't available, go one day back
                comb_path_1 = f"{comb_base_path}/_{dt_1}.parquet"
                if os.path.isfile(comb_path_1):
                    self.comb_path = comb_path_1
                    self.path = fpath_dict[which][f"{by}_1"]

    @classmethod
    def call_func(cls, self, which, by):
        """Determine class function to call."""
        if which == 'vol' and by == 'avg':
            self.call_vol_avg(self)

    @classmethod
    def call_vol_avg(cls, self):
        """Perform ops for highest volume/avg."""
        # Read all symbols fpath and filter to only common stock
        syms_fpath = f"{self.base_dir}/tickers/all_symbols.parquet"
        all_symbols = pd.read_parquet(syms_fpath)
        cs_syms = all_symbols[all_symbols['type'] == 'cs']['symbol'].tolist()

        # Read all iex_close data, sort to only common stock, create new column
        df = pd.read_parquet(self.comb_path)
        df = df[df['symbol'].isin(cs_syms)].copy(deep=True)
        df['vol/avg'] = (df['volume'] / df['avgTotalVolume'] * 100).round(0)
        df['changePercent'] = (df['changePercent'] * 100).round(1)
        df['ytdChange'] = (df['ytdChange'] * 100).round(1)
        vol_df = df.sort_values(by=['vol/avg'], ascending=False).head(50)
        vol_df.reset_index(drop=True, inplace=True)

        self.df = vol_df

    @classmethod
    def write_to_parquet(cls, self):
        """Write dataframe to local parquet file."""
        write_to_parquet(self.df, self.path)
