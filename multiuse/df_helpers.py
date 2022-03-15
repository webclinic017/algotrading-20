"""Store commonly used functions for dataframes in a class."""
# %% codecell

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.pd_funcs import vc
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from multiuse.pd_funcs import vc
    from multiuse.help_class import help_print_arg

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

    @staticmethod
    def combine_duplicate_columns(df, verbose=False):
        """Combine duplicate columns (merging errors)."""
        cols = df.columns

        cols_x = cols[cols.str.contains('_x', regex=True)]
        cols_y = cols[cols.str.contains('_y', regex=True)]
        cols_x_r = cols_x.str.replace('_x', '', regex=True)
        cols_y_r = cols_y.str.replace('_y', '', regex=True)

        # Check if there are duplicates from merge
        if not cols_x_r.difference(cols_y_r).empty:
            if verbose:
                msg1 = ("combine_duplicate_columns: No difference between "
                        "cols_x and cols_y")
                msg2 = (f"cols_x {str(cols_x)} and {str(cols_y)}")
                help_print_arg(f"{msg1}{msg2}")
            return df
        else:
            cols_new = cols_x_r
            for col in cols_new:
                # Create new column of all np.NaNs
                df[col] = np.NaN
                # Create list of old columns for easy acccess
                cols_dupe = [f"{col}_x", f"{col}_y"]
                idx = df[cols_dupe].dropna(how='all').index
                # Stack based on unique index, convert to series
                stacked = df.loc[idx][cols_dupe].stack().reset_index(1, drop=True)
                stacked = stacked[~stacked.index.duplicated()]
                # Set the non nan values equal to right index values
                df.loc[stacked.index, col] = stacked
                # Drop duplicate columns
                df.drop(columns=cols_dupe, inplace=True, errors='ignore')

        return df



# %% codecell
