"""Create local file structures."""
# %% codecell
###############################################
import os
from pathlib import Path
from datetime import date
import string
import glob

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg

# %% codecell
###############################################


def create_insider_trans_dirs():
    """Create insider trans directories 0-9."""
    base_trans_dirs = f"{baseDir().path}/sec/insider_trans"
    for num in list(range(10)):
        dir_fpath = f"{base_trans_dirs}/{num}"
        try:
            os.mkdir(dir_fpath)
        except FileExistsError:
            pass


def create_company_index_dirs():
    """Create insider trans directories 0-9."""
    base_ci_dirs = f"{baseDir().path}/sec/company_index"
    for num in list(range(10)):
        dir_fpath = f"{base_ci_dirs}/{num}"
        try:
            os.mkdir(dir_fpath)
        except FileExistsError:
            pass

# %% codecell
###############################################


def make_yfinance_dirs(temp=False):
    """Make options historical directory."""
    path = ''
    if not temp:
        path = Path(baseDir().path, 'derivatives/end_of_day')
    elif temp:
        path = Path(baseDir().path, 'derivatives/end_of_day/temp')
    makedirs_with_permissions(path)
    make_hist_prices_dir(path)


def create_ysymbol_info_dirs():
    """Create local fpath directory for ysymbol info."""
    base_dir = Path(baseDir().path, 'tickers/info')
    if not base_dir.exists():
        makedirs_with_permissions(base_dir)
        make_hist_prices_dir(base_dir)
        makedirs_with_permissions(Path(base_dir, 'combined'))
        makedirs_with_permissions(Path(base_dir, 'unfinished'))
        makedirs_with_permissions(Path(base_dir, 'temp'))


# %% codecell


def make_yearly_dir(base_path):
    """Make yearly directory."""
    # If base_path is not a directory
    if not os.path.isdir(base_path):
        makedirs_with_permissions(base_path)

    yr = date.today().year
    for year in list(range(yr, yr + 10)):
        makedirs_with_permissions(f"{base_path}/{year}")


def make_hist_prices_dir(base_path):
    """Make year and alphabet lowercase local folders."""
    # If base_path is not a directory
    if not os.path.isdir(base_path):
        makedirs_with_permissions(base_path)
    # First level is the years - this and the next 10

    yr = date.today().year
    for year in list(range(yr, yr + 10)):
        makedirs_with_permissions(f"{base_path}/{year}")

    # Get list of year paths
    yr_paths = glob.glob(f"{base_path}/**")

    for fpath in yr_paths:
        for letter in string.ascii_lowercase:
            makedirs_with_permissions(f"{fpath}/{letter}")


def makedirs_with_permissions(path):
    """Make directory with permissions."""
    if not os.path.isdir(path):
        os.umask(0)
        os.makedirs(path, mode=0o777)
    else:
        help_print_arg(f"Directory already exists: {str(path)}")
