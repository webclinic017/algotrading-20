"""Clear local data programatically."""
# Use with caution!

# %% codecell
####################################################################
import os
import glob

try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

# %% codecell
####################################################################


def remove_eod_quotes():
    """Remove all files in iex quotes directory."""
    for fpath in glob.glob(f"{baseDir().path}/iex_eod_quotes/*/**/***"):
        os.remove(fpath)
        print(fpath)


def remove_StockEOD():
    """Remove all files in StockEOD directory."""
    for fpath in glob.glob(f"{baseDir().path}/StockEOD/*/**/***"):
        os.remove(fpath)
        print(fpath)
# %% codecell
####################################################################

remove_eod_quotes()
