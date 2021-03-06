"""Functions to help with local fpaths."""
# %% codecell
import os
from pathlib import Path

from tqdm import tqdm
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
# %% codecell


def get_sizes(gz=False, parquet=True):
    """Get sizes of all files with x ending."""
    size_dict = {}

    file_ext = '.parquet'
    if gz:
        file_ext = '.gz'

    path_list = list(Path(baseDir().path).glob(f'**/*{file_ext}'))

    for fpath in tqdm(path_list):
        size = os.path.getsize(str(fpath)) / 1000000
        if size > 100:  # If size is greater than 100 mbs
            size_dict[str(fpath)] = size

    df_sizes = (pd.Series(size_dict)
                  .to_frame()
                  .reset_index()
                  .rename(columns={'index': 'fpath', 0: 'size'}))

    path_tw = Path(baseDir().path, 'errors', 'gz_sizes.parquet')
    df_sizes.to_parquet(path_tw)

# %% codecell


def paths_combine_dataframes(dirs, cb_path='', cb_all_path='', verbose=False):
    """Read dataframes and combine into combined, combined_all fpaths."""
    df_list = []

    for dir in dirs:
        path_list = dir.glob('**/*.parquet')
        for f in path_list:
            df_list.append(pd.read_parquet(f))

    df_all = pd.concat(df_list)

    df_cb = df_all[df_all['date'] == df_all['date'].max()]

    if verbose:
        msg = f"paths_combine_dataframes {str(df_cb['date'].max())}"
        help_print_arg(msg)

    write_to_parquet(df_cb, cb_path)
    write_to_parquet(df_all, cb_all_path)


# %% codecell

def concat_and_or_write(df_all, path, path_parq=True, path_gz=False, to_parq=True,
                        to_gz=False, from_parq=True, from_gz=False, verb=False):
    """Concat and write to parquet and/or gzip file."""

    if '.parquet' in str(path_parq):
        from_parq = True
    if '.gz' in str(from_gz):
        from_gz = True

    if verb:
        help_print_arg(path)

    if Path(path).exists() and from_parq:
        df_old = pd.read_parquet()
        df_all = pd.concat([df_old, df_all]).reset_index(drop=True)
        df_all.to_parquet(path)
    elif Path(path).exists() and from_gz:
        df_old = pd.read_json(path, compression='gzip')
        df_all = pd.concat([df_old, df_all]).reset_index(drop=True)
        df_all.to_json(path, compression='gzip')
    else:
        if to_parq:
            df_all.to_parquet(path)
        elif to_gz:
            df_all.to_json(path, compression='gzip')


# %% codecell


def get_most_recent_fpath(fpath_dir, f_pre='', f_suf='', dt='', this_year=True, second=False):
    """Get the most recent fpath in a directory."""
    path_to_return = False
    if not dt:  # If no date passed, default to iex_eod
        dt = getDate.query('iex_close')

    dt_list = getDate.get_bus_days(this_year=this_year)
    date_list = (dt_list[dt_list['date'].dt.date <= dt]
                 .sort_values(by=['date'], ascending=False))

    date_list['fpath'] = (date_list.apply(lambda row:
                                          f"_{row['date'].date()}",
                                          axis=1))

    date_list['fpath_yr'] = (date_list.apply(lambda row:
                                             f"_{row['date'].year}",
                                             axis=1))
    date_list['fpath_fmt'] = (date_list.apply(lambda row:
                                              f"_{row['date'].date().strftime('%Y%m%d')}",
                                              axis=1))

    # Iterate through dataframe to find the most recent
    for index, row in date_list.iterrows():
        tpath = Path(fpath_dir, f"{f_pre}{row['fpath']}{f_suf}.parquet")
        if tpath.exists():
            return tpath

    # Iterate through dataframe to find the most recent
    for index, row in date_list.iterrows():
        tpath = Path(fpath_dir, f"{f_pre}{row['fpath_yr']}{f_suf}.parquet")
        if tpath.exists():
            return tpath

    # Iterate through dataframe to find the most recent
    for index, row in date_list.iterrows():
        tpath = Path(fpath_dir, f"{f_pre}{row['fpath_fmt']}{f_suf}.parquet")
        if tpath.exists():
            return tpath

    if not path_to_return and not second:
        path_to_return = get_most_recent_fpath(fpath_dir, this_year=False, second=True)
        if path_to_return:
            help_print_arg(f"get_most_recent_fpath: first failed. Returning {str(path_to_return)}")
            return path_to_return

    if not path_to_return:
        msg_1 = "Directory empty or path doesn't follow format '_dt.parquet'. Returning first path"
        msg_2 = f": {fpath_dir}"
        help_print_arg(f"{msg_1} {msg_2}")

        paths = list(Path(fpath_dir).glob('*.parquet'))
        if paths:
            path_to_return = paths[-1]
            return path_to_return

# %% codecell
