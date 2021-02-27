"""
Nasdaq data pull.

Q - NASDAQ Global Select Market (NGS)
R - NASDAQ Capital Market
Short data comes out every day past 4:30 pm
"""

# %% codecell
############################################
import os
import sys
from io import BytesIO
import importlib

import datetime

import requests
import pandas as pd
import numpy as np


try:
    from scripts.dev.help_class import baseDir, dataTypes, getDate
    from scripts.dev.nasdaq_class import nasdaqShort
except ModuleNotFoundError:
    from help_class import baseDir, dataTypes, getDate
    from nasdaq_class import nasdaqShort
    importlib.reload(sys.modules['nasdaq_class'])
    from nasdaq_class import nasdaqShort

# %% codecell
############################################
rpt_date = getDate.query('occ')

short_td_df = nasdaqShort(rpt_date).df


# %% codecell
############################################
