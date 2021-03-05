"""
Nasdaq data classes.

Q - NASDAQ Global Select Market (NGS)
R - NASDAQ Capital Market
Short data comes out every day past 4:30 pm
"""

# %% codecell
############################################
import requests
import pandas as pd
import numpy as np

from io import BytesIO
import os

import datetime

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes


# %% codecell
############################################

class nasdaqShort():
    """Get daily circut breaker short data."""

    fbase = f"{baseDir().path}/short/daily_breaker"
    sh_base = "http://nasdaqtrader.com/dynamic/symdir/shorthalts/shorthalts"

    def __init__(self, rpt_date):
        self.rpt_date = rpt_date.strftime('%Y%m%d')
        self.df = self.get_data(self)

    @classmethod
    def get_data(cls, self):
        """Check for local json file."""
        self.fpath = f"{self.fbase}/nasdaq_{self.rpt_date}.gz"
        local_df = False

        if os.path.isfile(self.fpath):
            local_df = pd.read_json(self.fpath)
        else:
            local_df = self._get_request(self)
            self._write_to_json(self, local_df)

        return local_df

    @classmethod
    def _get_request(cls, self):
        """Get data from nasdaq circuit breaker."""
        sh_url = f"{self.sh_base}{self.rpt_date}.txt"

        sh_get = requests.get(sh_url)
        # Convert to pandas dataframe
        sh_df = self._convert_to_pandas(self, sh_get)

        return sh_df

    @classmethod
    def _convert_to_pandas(cls, self, sh_get):
        """Convert data to pandas dataframe."""
        sh_df = pd.read_csv(
                    BytesIO(sh_get.content),
                    escapechar='\n',
                    delimiter=',',
                    skipinitialspace=False
                    )
        # Convert object columns to category columns
        sh_df = dataTypes(sh_df).df

        return sh_df

    @classmethod
    def _write_to_json(cls, self, local_df):
        """Write to local json file."""
        local_df.to_json(self.fpath, compression='gzip')

















# %% codecell
############################################
