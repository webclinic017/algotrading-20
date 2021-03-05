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
from pathlib import Path

import datetime

import requests
import pandas as pd
import numpy as np


try:
    from scripts.dev.multiuse.help_class import baseDir, scriptDir, dataTypes, getDate
    from scripts.dev.data_collect.nasdaq_class import nasdaqShort
    from scripts.dev.data_collect.econ_class import yahooTbills
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, scriptDir, dataTypes, getDate

    from data_collect.nasdaq_class import nasdaqShort
    #importlib.reload(sys.modules['data_collect.nasdaq_class'])
    #from data_collect.nasdaq_class import nasdaqShort
    from data_collect.econ_class import yahooTbills
    importlib.reload(sys.modules['data_collect.econ_class'])
    from data_collect.econ_class import yahooTbills

# %% codecell
############################################

rpt_date = getDate.query('occ')

short_td_df = nasdaqShort(rpt_date).df


# %% codecell
############################################
