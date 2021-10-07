"""Clear local data programatically."""
# Use with caution!

# %% codecell
####################################################################
import os
import glob
from pathlib import Path

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate

# %% codecell
####################################################################

# %% codecell


def remove_eod_quotes():
    """Remove all files in iex quotes directory."""
    for fpath in glob.glob(f"{baseDir().path}/iex_eod_quotes/*/**/***"):
        # os.remove(fpath)
        print(fpath)

# %% codecell


def remove_StockEOD():
    """Remove all files in StockEOD directory."""
    for fpath in glob.glob(f"{baseDir().path}/StockEOD/*/**/***"):
        os.remove(fpath)
        print(fpath)

# %% codecell


def clear_yoptions_dirs():
    """Removing files in yoptions due to incompatibility."""
    path = Path(baseDir().path, 'derivatives/end_of_day/2021')
    path_list = list(path.glob('**/*.parquet'))

    for fpath in path_list:
        os.remove(fpath)

# %% codecell


def clear_yoptions_temp_unfin(testing=False):
    """Clear temp and unfin yoptions directories."""
    dt = getDate.query('iex_eod')
    yr = str(dt.year)
    path = Path(baseDir().path, 'derivatives/end_of_day/temp', yr)
    temps = list(path.glob('**/*.parquet'))

    if temps:
        for fpath in temps:
            if testing:
                print(fpath)
                break
            else:
                os.remove(fpath)

    unfin = Path(baseDir().path, 'derivatives/end_of_day/unfinished')
    unfins = list(unfin.glob('*.parquet'))

    if unfins:
        for fpath in unfins:
            if testing:
                print(fpath)
                break
            else:
                os.remove(fpath)


# %% codecell
####################################################################
