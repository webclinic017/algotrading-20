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

try:
    from scripts.dev.help_class import dataTypes, baseDir
    #from scripts.dev.help_class import dataTypes, baseDir
except ModuleNotFoundError: 
    try:
        from help_class import dataTypes, baseDir
    except ModuleNotFoundError:
        print('Error from stocktwits.py')
finally:
    print(str(Path(os.getcwd())))
# %% codecell
#############################################################

class stTrending():
    """Stock twits trending data."""
    st_base_url = "https://api.stocktwits.com/api/2/"
    st_base_path = f"{baseDir().path}/stocktwits/trending"

    def __init__(self):
        self.rec_df = self.request_data(self)
        self.format_data(self)
        self.check_for_exist(self)
        self.df = self.concat_and_write(self)


    @classmethod
    def request_data(cls, self):
        """Request data from stocktwits api."""
        # Trending stocks
        trend_u1 = "trending/symbols/equities.json"
        trend_url = f"{self.st_base_url}{trend_u1}"
        payload = {'limit': 29}

        stwits_get = requests.get(trend_url, params=payload)
        stwits = json.load(StringIO(stwits_get.content.decode('utf-8')))
        stwits_df = pd.DataFrame(stwits['symbols'])
        return stwits_df

    @classmethod
    def format_data(cls, self):
        """Format stocktwits data."""
        self.rec_df.drop(columns=['is_following', 'title', 'aliases'], inplace=True)
        self.rec_df = dataTypes(self.rec_df).df
        self.rec_df['timestamp'] = pd.Timestamp.now('US/Eastern')

    @classmethod
    def check_for_exist(cls, self):
        """Check existing dataframe."""
        # Construct file path
        self.st_fname = f"{self.st_base_path}/{date.today()}.gz"

        if os.path.isfile(self.st_fname):
            self.old_df = pd.read_json(self.st_fname, compression='gzip')
        else:
            self.old_df = pd.DataFrame()

    @classmethod
    def concat_and_write(cls, self):
        """Concat dataframes and write to json."""
        new_df = pd.concat([self.old_df, self.rec_df])
        new_df.reset_index(inplace=True, drop=True)
        # Write data to local json file
        new_df.to_json(self.st_fname, compression='gzip')

        return new_df

# %% codecell
#############################################################
