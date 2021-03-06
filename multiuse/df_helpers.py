"""Store commonly used functions for dataframes in a class."""
# %% codecell
from io import StringIO
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
        fname = 'combine_duplicate_columns'

        c_all = df.columns
        col_x = c_all[c_all.str.contains('_x', regex=True)]
        col_y = c_all[c_all.str.contains('_y', regex=True)]
        cols_ = col_x.str.replace('_x', '', regex=True)

        cols_to_replace = []

        for col in cols_:
            colsx = col_x[col_x.str.contains(col)]
            colsy = col_y[col_y.str.contains(col)]

            if colsx.empty:
                col_y = col_y.drop(f"{col}_y", errors='ignore')
                cols_to_replace.append(f"{col}_y")
                if verbose:
                    help_print_arg(f"{fname}: dropping {col}_y")
            elif colsy.empty:
                col_x = col_x.drop(f"{col}_x", errors='ignore')
                cols_to_replace.append(f"{col}_x")
                if verbose:
                    help_print_arg(f"{fname}: dropping {col}_x")

        ctr = pd.Index(cols_to_replace)
        ctr = ctr.str.replace('_y|_x', '', regex=True)
        col_dict = {k: v for k, v in zip(cols_to_replace, ctr)}
        # Get the difference between teh column indices
        cols_ = cols_.difference(ctr)

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

            df.drop(columns=col_x.append(col_y), errors='ignore', inplace=True)
            df.rename(columns=col_dict, inplace=True)

        return df

    @staticmethod
    def standardize_df_list(df_list, **kwargs):
        """Standardize data types (categorical) in df_list."""
        # In preparation for dask, but with a list of dataframes pre concat
        verbose = kwargs.get('verbose', False)

        df0 = df_list[0]
        cols_cat = df0.dtypes[df0.dtypes == 'category'].index
        df_list_fine = []

        for n, df_n in tqdm(enumerate(df_list)):
            cols_cat_test = df_n.dtypes[df_n.dtypes == 'category'].index

            # Get master list of all category columns
            if not cols_cat.equals(cols_cat_test):
                cols_cat = cols_cat.append(cols_cat_test).drop_duplicates()
                continue
            else:
                df_list_fine.append(df_n)
                # Remove dataframe by index position
                df_list.pop(n)

        for n, df_n in tqdm(enumerate(df_list)):
            cols_cat_test = df_n.dtypes[df_n.dtypes == 'category'].index
            cols_diff = cols_cat.difference(cols_cat_test).tolist()
            # Convert columns to categorical for each of the dataframes
            if not cols_cat.empty and not cols_diff:
                df_list[n][cols_diff] = (df_list[n][cols_diff]
                                         .astype('category'))

        df_list = df_list + df_list_fine
        # Optional testing
        # dd_all = dd.concat(tc.path_list)
        return df_list

    @staticmethod
    def df_info_to_df_info(df):
        """Dataframe info to returnable dataframe."""

        buf = StringIO()
        df.info(buf=buf, verbose=True, show_counts=True)
        s = buf.getvalue()
        lines = buf.getvalue().splitlines()

        dft1 = pd.DataFrame(lines[5:-2])[0].str.split(' ', expand=True)
        s = dft1.to_numpy()
        orders = np.argsort(s == '', axis=1, kind='mergesort')
        dft1[:] = s[np.arange(len(s))[:, None], orders]

        dft2 = (dft1.replace(['None', ''], np.NaN)
                    .dropna(how='all', axis=1)
                    .drop(columns=[5], errors='ignore'))

        cols = list(filter(None, lines[3].split(' ')))
        dft2.columns = cols
        dft2.drop(columns=['#'], errors='ignore', inplace=True)

        dft2['Non-Null'] = dft2['Non-Null'].astype('int')

        dfl = dft2['Non-Null'].max()
        dft2['null%'] = dft2['Non-Null'].div(dfl)

        return dft2



# %% codecell
