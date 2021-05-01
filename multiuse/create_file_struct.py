"""Create local file structures."""
# %% codecell
###############################################
import os

try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

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

# %% codecell
###############################################
