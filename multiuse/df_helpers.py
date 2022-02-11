"""Store commonly used functions for dataframes in a class."""
# %% codecell

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.pd_funcs import vc
except ModuleNotFoundError:
    from multiuse.pd_funcs import vc

pd.DataFrame.vc, pd.Series.vc = vc, vc

# %% codecell


class DfHelpers():
    """Collection of useful funcs."""

    @staticmethod
    def check_nan(a, b=np.NaN):
        """Check nans."""
        try:
            if a.dtype == 'O':
                if a.str.contains('NaN').any():
                    a = a.astype(np.float64)
        except AttributeError:
            pass

        return (a == b) | ((a != a) & (b != b))

    @staticmethod
    def round_cols(df, cols=False, decimals=3):
        """Round specified columns, or round all columns."""

        if not cols:
            cols = (df.select_dtypes(include=[np.float32, np.float64])
                      .columns.tolist())

        df[cols] = df[cols].astype(np.float64)
        df[cols] = df[cols].round(decimals)

        return df

    @staticmethod
    def remove_cats_no_counts(df):
        """Remove categorical columns without any data values, from index."""
        cat_cols = df.select_dtypes(include='category').columns
        for col in cat_cols:
            vct = df[col].vc()
            if vct[vct == 0].count() > 0:
                df[col] = df[col].astype('object').astype('category')

        return df



# %% codecell



# %% codecells
