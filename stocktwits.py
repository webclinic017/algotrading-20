"""
Let's have some fun with stocktwits data
- Rate limit of 200 get requests/hour with unauthenticated
- Rate limit of 400 get requests/hour with authenticated
- Unauthenticated is tied to IP address (rotating proxies)
"""
# %% codecell
#############################################################
import requests
import pandas as pd
import numpy as np
import json
from io import StringIO, BytesIO
import os.path
from pathlib import Path
import os

import sys
import importlib

import datetime
from datetime import date, timedelta, time

from nested_lookup import nested_lookup
from nested_lookup import get_occurrence_of_key as gok

try:
    from scripts.dev.help_class import dataTypes, baseDir
    from scripts.dev.stocktwits_class import stwitsUserStream
except ModuleNotFoundError:
    from help_class import dataTypes, baseDir
    from stocktwits_class import stwitsUserStream

# %% codecell
#############################################################

# Do this twice a day, once in the morning, once at night
# Get the last 29 messages. That seems like it would be enough

from stocktwits_class import stwitsUserStream
importlib.reload(sys.modules['stocktwits_class'])
from stocktwits_class import stwitsUserStream


# %% codecell
#############################################################

# %% codecell
#############################################################
























# %% codecell
#############################################################
