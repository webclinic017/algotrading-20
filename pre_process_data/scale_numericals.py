"""Scale/denoise numerical data."""
# %% codecell
from pathlib import Path
import typing

import pandas as pd
from pykalman import KalmanFilter
import pywt
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet

# %% codecell


class SklearnWrapper:
    def __init__(self, transform: typing.Callable, cols):
        self.transform = transform
        self.cols = cols

    def __call__(self, df):
        transformed = self.transform.fit_transform(df[self.cols].values)
        return pd.DataFrame(transformed, columns=self.cols, index=df.index)


class ScaleTransform():
    """Apply different methods of scaling data."""

    cols_to_scale = []
    fib_cols = []
    cols = []
    df = pd.DataFrame()
    sk_wrapper = SklearnWrapper

    def __init__(self, df, standard=True):
        self.standard = standard
        self._cols_to_scale(self)
        self._create_fib_col_list(self, df)
        self._convert_fib_cols_to_rel_percs(self, df)

        if len(df['symbol'].unique()) > 1:
            self.df = self._standard_scaler(self, self.df, self.cols)
        else:
            self.df = self._apply_wrapper(self, self.df, self.cols)

    @classmethod
    def _cols_to_scale(cls, self):
        """List of columns to scale."""
        cols_to_scale = (['fOpen', 'fHigh', 'fLow', 'fClose', 'gap',
                          'fVolume', 'fRange', 'fChangeP', 'vol/mil',
                          'prev_close',
                          'gRange', 'cumPerc', 'perc5', 'vol_avg_2m', 'fCP5',
                          'fHighMax', 'rsi', 'sma_50',
                          'sma_200', 'fibHighMinVal', 'fibLowMinVal',
                          'fibHighDiffP', 'fibLowDiffP', 'pp_fHigh', 'pp_fLow',
                          'vol/avg2m', 'stdDev', 'annStdDev', 'perc1', 'perc2',
                          'perc3', 'perc10', 'perc20', 'h/l_perc1',
                          'h/l_perc2',
                          'h/l_perc3', 'fibCloseVal', 'fibPP'])

        self.cols_to_scale = cols_to_scale
        self.cols = self.cols + cols_to_scale

    @classmethod
    def _create_fib_col_list(cls, self, df):
        """Create fibonacci column list to scale."""
        col_list = df.columns.tolist()

        exts_to_remove = ['ext_date', 'ext_end', 'ext_cond']
        exts = ([col for col in col_list if 'ext_'
                 in col if col not in exts_to_remove
                 if 'ext_date' not in col])
        rets = [col for col in col_list if 'ret_' in col]

        # Excluded start, high, and ext_end columns
        other_fib_cols = (['fibPercRange', 'fib_range'])
        fib_cols = exts + rets + other_fib_cols

        self.fib_cols = fib_cols
        self.cols = self.cols + fib_cols

    @classmethod
    def _convert_fib_cols_to_rel_percs(cls, self, df):
        """Get the relative % difference between the fClose column."""
        fib_cols = self.fib_cols
        fibs_to_exclude = ['fibPercRange', 'fib_range']
        fib_cols_sub = [col for col in fib_cols if col not in fibs_to_exclude]

        for col in fib_cols_sub:
            df[col] = (df[col].sub(df['fClose']) / df['fClose'])

        self.df = df.copy()

    @classmethod
    def _standard_scaler(cls, self, df, cols):
        """Apply standard scaler to columns, return df."""
        scaler = StandardScaler()
        data = df[cols].fillna(0)
        sc = scaler.fit_transform(data.to_numpy())
        df[cols] = sc

        return df

    @classmethod
    def _apply_wrapper(cls, self, df, cols):
        """Apply wrapper for bulk data operation."""
        df_rescaled = (
            df.groupby("symbol")
            .apply(self.sk_wrapper(StandardScaler(), cols))
        )
        og_cols = df.columns
        other_cols = og_cols.difference(cols)
        df_rescaled = df[other_cols].join(df_rescaled)

        return df_rescaled


# %% codecell
