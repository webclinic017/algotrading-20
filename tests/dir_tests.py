"""
Get a list of all directories, create in new dir data_test
"""
# %% codecell
##############################
import sys
import glob
import os

from multiuse.help_class import baseDir
# %% codecell
##############################
data_dir = f"{baseDir().path}/*"

glob.glob(data_dir)


list(os.walk(baseDir().path))

# %% codecell
##############################
