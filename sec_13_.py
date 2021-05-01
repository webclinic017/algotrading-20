"""Workbook for institutional ownership changes."""
# %% codecell
#####################################################
import time
import os.path, os
import importlib
import sys

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import bs4.element

from data_collect.sec_routines import get_cik, secInsiderTrans
importlib.reload(sys.modules['data_collect.sec_routines'])
from data_collect.sec_routines import get_cik, secInsiderTrans

from multiuse.help_class import baseDir

from multiuse.bs4_funcs import bs4_child_values
importlib.reload(sys.modules['multiuse.bs4_funcs'])
from multiuse.bs4_funcs import bs4_child_values

# %% codecell
#####################################################
# Form 13G 13G/A 13D/A


# %% codecell
#####################################################
