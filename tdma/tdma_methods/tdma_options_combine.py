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
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg
    from multiuse.class_methods import ClsHelp
    from multiuse.df_helpers import DfHelpers

# pd.set_option('display.max_columns', None)
# tc = TdmaCombine(method='options_chain', use_dask=True, verbose=True)
# %% codecell


class TdmaCombine(ClsHelp):
    """Combine TDMA options EOD."""

    def __init__(self, method, **kwargs):
        self._get_class_vars(self, **kwargs)
        if method == 'options_chain':
            self._combine_options_chain(self)

        self.sdict = ({
            'path_list': self.path_list,
            'f_combined': self.fpath_combined,
            'f_combined_all': self.fpath_combined_all,
            'use_dask': self.use_dask})

        if self.path_list:
            self.df_list_err = (_concat_and_combine(method, self.sdict))

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.use_dask = kwargs.get('use_dask', False)
        self.dt = kwargs.get('dt', getDate.query('mkt_open'))
        self.yr = self.dt.year

        self.bdir_dervs = Path(baseDir().path, 'derivatives', 'tdma')
        self.fdir_series = self.bdir_dervs.joinpath('series', str(self.yr))

        self.fpath_combined = (self.fdir_series.parent
                               .joinpath('combined', f"_{self.dt}.parquet"))
        self.fpath_combined_all = (self.fdir_series.parent.joinpath
                                   ('combined_all', f'_{self.yr}.parquet'))

        if self.verbose:
            msg1 = f"TdmaCombine {str(self.fpath_combined)}"
            help_print_arg(f"{msg1} {str(self.fpath_combined_all)}")

    @classmethod
    def _combine_options_chain(cls, self):
        """Combine options chain data and write locally."""
        if self.verbose:
            help_print_arg(f"TdmaCombine: path_dir = {str(self.fdir_series)}")
        # Get all paths ending in .parquet for directory
        paths = list(self.fdir_series.rglob('*.parquet'))
        path_list = []
        # Read the first path, convert into dataframe
        df_0 = pd.read_parquet(paths[0])
        # Check if date is in the columns
        if 'date' not in df_0.columns:
            msg1 = "_combine_options_chain: date not in col"
            help_print_arg(f"{msg1} cols {str(df_0.columns.tolist())}")
            self.df_0 = df_0
            help_print_arg('First dataframe accessible under self.df_0')
        # Iterate through paths. Add dataframes to class list
        for fpath in tqdm(paths):
            try:
                path_list.append(pd.read_parquet(fpath))
            except Exception as e:
                self.elog(self, e)
        # Assign class list to self variable
        self.path_list = path_list


def _concat_and_combine(method, sdict):
    """Concat, combine, and write to local dataframe."""
    # Where sdict is limited dictionary of prev class attributes
    path_list = sdict['path_list']
    f_combined_all = sdict['f_combined_all']
    f_combined = sdict['f_combined_all']
    use_dask = sdict['use_dask']

    df_all, dd_all, dates = None, None, None
    df_list_err = []
    if use_dask:
        # Standardize categorical data types for dask
        path_list = DfHelpers.standardize_path_list(path_list)

        dd_all = path_list[0]
        for f in tqdm(path_list[1:]):
            try:
                dd_all = dd.concat([dd_all, f])
            except Exception as e:
                print(f"{type(e)} {str(e)}")
                df_list_err.append(f)

                if len(df_list_err) < 50:
                    continue
                else:
                    return df_list_err

        if 'date' not in dd_all.columns:
            dd_all['date'] = dd_all['quoteTimeInLong'].dt.date
        dates = dd_all['date'].unique().compute().tolist()
    else:
        df_all = pd.concat(path_list)
        if 'date' not in df_all.columns:
            df_all['date'] = df_all['quoteTimeInLong'].dt.date
        dates = df_all['date'].unique()

    # Iterate through unique dates, if date list exists
    if dates.size >= 1:  # np.array format
        for dt in tqdm(dates):
            if use_dask:
                _write_each_date(dt, dd_all, f_combined, use_dask)
            else:
                _write_each_date(dt, df_all, f_combined, use_dask)
    else:  # Print to console that the date list doesn't exist
        msg = "TMDA Options Combine: _concat_and_combine - not date list"
        help_print_arg(msg)
        return df_list_err

    if use_dask:  # Compute dataframe before writing locally
        df_all = dd_all.compute().copy()
    write_to_parquet(df_all, f_combined_all)
    return df_list_err


def _write_each_date(dt, df_all, f_combined, use_dask):
    """Write each date in the correct directory."""
    df = None
    f_date = f_combined.parent.joinpath(f"_{dt}.parquet")

    try:
        df = df_all[df_all['date'].dt.date == dt]
    except AttributeError:
        df = df_all[df_all['date'] == dt]

    if use_dask:
        df = df.compute()

    if not df.empty:
        write_to_parquet(df, f_date)

# %% codecell






# %% codecell
