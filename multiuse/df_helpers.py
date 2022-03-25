"""Store commonly used functions for dataframes in a class."""
# %% codecell

import pandas as pd
import numpy as np
from tqdm import tqdm

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
        if isinstance(df.index, pd.Index):
            df.reset_index(drop=True, inplace=True)

        c_all = df.columns
        col_x = c_all[c_all.str.contains('_x', regex=True)]
        col_y = c_all[c_all.str.contains('_y', regex=True)]
        cols_ = col_x.str.replace('_x', '', regex=True)

        # Check if there are duplicates from merge
        if col_x.empty:
            if verbose:
                msg1 = ("combine_duplicate_columns: No difference between "
                        "cols_x and cols_y")
                msg2 = (f"cols_x {str(col_x)} and {str(col_y)}")
                help_print_arg(f"{msg1}{msg2}")
            return df
        else:
            for col in cols_:
                colm = df.get(col, f"{col}_x")
                if isinstance(colm, pd.Series):
                    colm = colm.name
                vals = (np.where(
                                df[colm].notna(),
                                df[colm],
                                np.where(
                                    df[f"{col}_x"].notna(), df[f"{col}_x"],
                                    np.where(
                                        df[f"{col}_y"].notna(), df[f"{col}_y"], 0
                                    ),
                                ),
                            ))
                df[col] = vals

            df.drop(columns=col_x.append(col_y), inplace=True)

        return df

    @staticmethod
    def standardize_path_list(path_list):
        """Standardize data types (categorical) in path_list."""
        # In preparation for dask

        df0 = path_list[0]
        cols_cat = df0.dtypes[df0.dtypes == 'category'].index
        for df_n in tqdm(path_list):
            cols_cat_test = df_n.dtypes[df_n.dtypes == 'category'].index

            # Get master list of all category columns
            if not cols_cat.equals(cols_cat_test):
                cols_cat = cols_cat.append(cols_cat_test).drop_duplicates()
                continue
        # Convert columns to categorical for each of the dataframes
        for n, df_n in tqdm(enumerate(path_list)):
            path_list[n][cols_cat] = path_list[n][cols_cat].astype('category')

        # Optional testing
        # dd_all = dd.concat(tc.path_list)
        return path_list



# %% codecell
