"""TDMA Options Analysis."""
# %% codecell

from pathlib import Path

import pandas as pd
from tqdm import tqdm
import dask.dataframe as dd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.multiuse.df_helpers import DfHelpers
    from scripts.dev.multiuse.pathClasses.construct_paths import PathConstructs
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from multiuse.class_methods import ClsHelp
    from multiuse.df_helpers import DfHelpers
    from multiuse.pathClasses.construct_paths import PathConstructs

# pd.set_option('display.max_columns', None)
# tc = TdmaCombine(method='options_chain', use_dask=True, verbose=True)
# %% codecell


class TdmaCombine(ClsHelp):
    """Combine TDMA options EOD."""

    def __init__(self, method, **kwargs):
        self._tc_get_class_vars(self, **kwargs)
        if method in ['options_chain', 'combine_options']:
            # Get class variables for combine_options method
            self._tc_options_vars(self)
            # If fix local data types, iterate through paths and fix files
            if self.fix_local_dtypes:
                self._tc_fix_local_tdma_options(self, **kwargs)

            # Proceed to combining the options
            self._tc_combine_options_chain(self, self.paths)
            self.sdict = ({
                'df_list': self.df_list,
                'f_combined': self.fpath_combined,
                'f_combined_all': self.fpath_combined_all,
                'use_dask': self.use_dask,
                'verbose': self.verbose})
        else:
            help_print_arg(f"TdmaCombine: no matching method. You put {str(method)}")

        if self.df_list:
            self.df_list_err = (concat_and_combine(method, self.sdict))

    @classmethod
    def _tc_get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.use_dask = kwargs.get('use_dask', False)
        # For whether or not to iterate through local files, fix datatypes
        self.fix_local_dtypes = kwargs.get('fix_local_dtypes', False)
        self.dt = kwargs.get('dt', getDate.query('mkt_open'))
        self.yr = self.dt.year

        if self.verbose and self.fix_local_dtypes:
            help_print_arg(f"TdmaCombine: fix_local_dtypes {str(self.fix_local_dtypes)}")

    @classmethod
    def _tc_options_vars(cls, self):
        """Get fpaths, other for tdma options."""
        self.bdir_dervs = Path(baseDir().path, 'derivatives', 'tdma')
        self.fdir_series = self.bdir_dervs.joinpath('series')

        self.fpath_combined = (self.fdir_series.joinpath('combined',
                               str(self.yr), f"_{self.dt}.parquet"))
        self.fpath_combined_all = (self.fdir_series.joinpath(
                                   'combined_all', f'_{str(self.yr)}.parquet'))
        # Get all paths ending in .parquet for directory
        self.paths = PathConstructs.glob_all_non_combined(self.fdir_series)

        self.df_list = []

        if self.verbose:
            help_print_arg(f"""TdmaCombine: path_dir = {str(self.fdir_series)}
                           TdmaCombine {str(self.fpath_combined)}
                           {str(self.fpath_combined_all)}""")

            if not self.paths:
                help_print_arg("TdmaCombine paths empty")

    @classmethod
    def _tc_fix_local_tdma_options(cls, self, **kwargs):
        """Standardizing categorical columns for dataframes."""
        # This should only need to happen once
        print('Starting: _fix_local_tdma_options')
        # Columns to convert to categorical
        cols_to_cat = (['putCall', 'symbol', 'description', 'exchangeName',
                        'bidAskSize', 'optionDeliverablesList',
                        'expirationType', 'settlementType', 'deliverableNote',
                        'underlying', 'dt_symbol', 'date'])

        for f in tqdm(self.paths):
            try:
                fix_local_file_datatypes(f, cols_to_cat, **kwargs)
            except Exception as e:
                self.elog(self, e)

    @classmethod
    def _tc_combine_options_chain(cls, self, paths):
        """Combine options chain data and write locally."""
        df_list = []
        # Read the first path, convert into dataframe
        df_0 = pd.read_parquet(paths[0])
        # Check if date is in the columns
        if 'date' not in df_0.columns:
            msg1 = "_combine_options_chain: date not in col"
            help_print_arg(f"{msg1} cols {str(df_0.columns.tolist())}")
            self.df_0 = df_0
            help_print_arg('First dataframe accessible under self.df_0')

        if self.verbose:
            help_print_arg(f"""_tc_combine_options_chain: Reading paths
                           + adding to df_list with dask ==
                           {str(self.use_dask)}""")
        # Iterate through paths. Add dataframes to class list
        if self.use_dask:
            for fpath in tqdm(paths):
                try:
                    df_list.append(dd.read_parquet(fpath))
                except Exception as e:
                    self.elog(self, e)
                    break
        else:
            for fpath in tqdm(paths):
                try:
                    df_list.append(pd.read_parquet(fpath))
                except Exception as e:
                    self.elog(self, e)
                    break
        # Assign class list to self variable
        self.df_list = df_list

        df_list_cols = []
        for df_n in self.df_list:
            df_list_cols.append(df_n.columns)

        self.df_list_cols = df_list_cols

# %% codecell


def fix_local_file_datatypes(fpath, cols_to_cat, **kwargs):
    """Fix local datatypes for columns."""
    df = pd.read_parquet(fpath).copy()
    if 'date' not in df.columns:
        df['date'] = df['quoteTimeInLong'].dt.date
    df[cols_to_cat] = df[cols_to_cat].copy()
    write_to_parquet(df, fpath)


def concat_and_combine(method, sdict):
    """Concat, combine, and write to local dataframe."""
    # Where sdict is limited dictionary of prev class attributes
    df_list = sdict['df_list']
    f_combined = sdict['f_combined']
    f_combined_all = sdict['f_combined_all']
    use_dask = sdict['use_dask']
    verbose = sdict['verbose']

    df_all, dd_all, dates = None, None, None
    df_list_err = []

    if verbose:
        help_print_arg(f"""concat_and_combine:
                       Trying to concat dataframes.
                       Dask == {str(use_dask)}""")

    if use_dask:
        try:
            dd_all = dd.concat(df_list)
        except Exception as e:
            help_print_arg(f"""Could not dask.concat df_list
                           with error: {type(e)} {str(e)}""")
            if verbose:
                help_print_arg("""concat_and_combine: starting
                               DfHelpers.standardize_df_list""")

            # Standardize categorical data types for dask
            df_list = DfHelpers.standardize_df_list(df_list)

            fdir = Path(baseDir().path, 'derivatives', 'tdma', 'series', '2022')
            f_t = fdir.joinpath('t', '_TSLA.parquet')
            dd_all = dd.read_parquet(f_t) if f_t.exists() else df_list[0]

            """
            # Concat as many dataframes before error
            for n1, df_n in enumerate(df_list):
                try:
                    dd_all = dd.concat([dd_all, df_n])
                except Exception as e:
                    # help_print_arg(f"TDMA :concat_and_combine : made it to concat iteration {str(n1)} before break")
                    break
            """

            for n2, df_n in tqdm(enumerate(df_list)):
                try:
                    dd.concat([dd_all, df_n])
                except Exception as e:
                    print(f"{type(e)} {str(e)}")
                    # Add to list of errors
                    df_list_err.append(df_n)
                    # Remove dataframe from list
                    df_list.pop(n2)

                    if len(df_list_err) < 50:
                        continue
                    else:
                        help_print_arg("""concat_and_combine: reached
                                       > 50 errors while trending to
                                       dask.concat dataframes""")
                        return df_list_err

            # Trying concatting dataframe list again
            dd_all = dd.concat(df_list)

        if 'date' not in dd_all.columns:
            dd_all['date'] = dd_all['quoteTimeInLong'].dt.date
        try:
            dates = dd_all['date'].unique().compute().tolist()
        except TypeError as te:
            help_print_arg("""TypeError: dates =
                           dd_all['date'].unique().compute().tolist()
                           : could not compute dd_all""")
            return dd_all
    else:
        df_all = pd.concat(df_list)
        if 'date' not in df_all.columns:
            df_all['date'] = df_all['quoteTimeInLong'].dt.date
        dates = df_all['date'].unique().tolist()

    # Iterate through unique dates, if date list exists
    if len(dates) >= 1:  # list rather than np.array
        if verbose:
            help_print_arg("concat_and_combine: iterating through date list")
        for dt in tqdm(dates):
            if use_dask:
                try:
                    write_each_date(dt, dd_all, f_combined, use_dask)
                except Exception as e:
                    help_print_arg(f"Error: write_each_date: {type(e)} {str(e)}")
                    help_print_arg(f"date: {str(dt)}. Returning dd_all")
                    return dd_all
            else:
                write_each_date(dt, df_all, f_combined, use_dask)
    else:  # Print to console that the date list doesn't exist
        msg = "TMDA Options Combine: _concat_and_combine - not date list"
        help_print_arg(msg)
        return df_list_err

    if use_dask:  # Compute dataframe before writing locally
        df_all = dd_all.compute().copy()
    write_to_parquet(df_all, f_combined_all)
    return df_list_err


def write_each_date(dt, df_all, f_combined, use_dask):
    """Write each date in the correct directory."""
    df = None
    f_date = f_combined.parent.joinpath(f"_{dt}.parquet")

    try:
        df = df_all[df_all['date'].dt.date == dt]
    except AttributeError:
        df = df_all[df_all['date'] == dt]

    if use_dask:
        df = df.compute()
        # There may be an option to specify the number of categories
        # categories={'name':80000}

    if not df.empty:
        write_to_parquet(df, f_date)

# %% codecell






# %% codecell
