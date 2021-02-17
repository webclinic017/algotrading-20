"""
Get economic data and store to local json file.
"""
# %% codecell
############################################################
import os
import json
from json import JSONDecodeError
from io import StringIO, BytesIO
import glob
import importlib
import sys
import cProfile

import os.path

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

import datetime
from datetime import date, timedelta, time

# Derivatives data
from options import DerivativesHelper, DerivativesStats
from file_storage import fileOps, blockPrinting

importlib.reload(sys.modules['options'])
importlib.reload(sys.modules['file_storage'])

# %% codecell
############################################################
"""
Get the last year of 3-month T bill rates (risk free).
Write function to find the most recent date and then get all data in
between today and that day.
"""

class marketHolidays():
    """Helper class for finding last trading date/market holidays."""
    # type can be 'trade' or 'holiday'
    # Dataframe is stored as days_df
    def __init__(self, type):
        self.days_df = self.check_if_exists(self, type)
        if not isinstance(self.days_df, (bool)):
            self.params = self.determine_params(self, type)
            self.full_url = self.construct_url(self, self.params)
            self.days_df = self.get_market_trading_dates(self)
            self.write_to_json(self)

    @classmethod
    def check_if_exists(cls, self, type):
        """Check if requested file exists locally."""
        base_dir = f"{Path(os.getcwd()).parents[0]}/data/economic_data"

        if type == 'trade':
            fname = f"{base_dir}/{date.today()}_{'trade'}.json"
        elif type == 'holiday':
            fname = f"{base_dir}/{date.today().year}_{'holiday'}.json"

        if os.path.isfile(fname):
            local_df = pd.read_json(fname)
        else:
            local_df = False

        return local_df


    @classmethod
    def determine_params(cls, self, type):
        """Create dictionary of parameters."""
        params = {'type': type, 'direction': '', 'last': 0, 'startDate': 'YYYYMMDD'}
        if type == 'trade':
            params['direction'] = 'last'
            params['last'] = 1
        elif type == 'holiday':
            params['direction'] = 'next'
            next_year = (date.today() + timedelta(days=365)).year
            params['last'] = (datetime.date(next_year, 1, 1,) - date.today()).days

        else:
            print('type needs to be either trade or holiday')
            return False

        params['startDate'] = date.today().strftime("%Y%m%d")
        return params

    @classmethod
    def construct_url(cls, self, params):
        """Construct url to call get request."""
        load_dotenv()
        base_url = os.environ.get("base_url")
        url_1 = f"/ref-data/us/dates/{params['type']}/{params['direction']}"
        url_2 = f"/{params['last']}/{params['startDate']}"
        full_url = f"{base_url}{url_1}{url_2}"

        return full_url

    @classmethod
    def get_market_trading_dates(cls, self):
        """Get market holiday data from iex."""
        payload = {'token': os.environ.get("iex_publish_api")}
        td_get = requests.get(
            self.full_url,
            params=payload  # Passed through function arg
            )
        # Convert bytes to dataframe=
        df = pd.json_normalize(json.loads(td_get.content))

        return df

    @classmethod
    def write_to_json(cls, self):
        """Write date to local json file."""
        base_dir = f"{Path(os.getcwd()).parents[0]}/data/economic_data"

        if self.params['type'] == 'trade':
            fname = f"{base_dir}/{date.today()}_{'trade'}.json"
        elif self.params['type'] == 'holiday':
            fname = f"{base_dir}/{date.today().year}_{'holiday'}.json"

        self.days_df.to_json(fname)



def get_tdata(payload, base_url):
    """Get tdata from IEX with specified range."""
    # 3 month risk free rate
    symbol = "DGS3MO"
    tdata = requests.get(
        f"{base_url}/time-series/treasury/{symbol}",
        params=payload  # Passed through function arg
        )
    print(f"Trying to get new data with the parameters {payload}")
    new_tdata = pd.json_normalize(json.loads(tdata.content))
    new_tdata['dt_date'] = pd.to_datetime(new_tdata['date'], unit='ms')
    return new_tdata


def read_tdata():
    """Read local data or get if not available."""
    load_dotenv()
    base_url = os.environ.get("base_url")

    # Load base_directory (derivatives data)
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/economic_data"
    choices = glob.glob(f"{base_dir}/*")
    fname = f"{base_dir}/risk_free_daily.json"

    if fname in choices:  # If file is saved locally
        tdata_df = pd.read_json(fname)

        # Most recent date in local data
        try:
            mr_date = tdata_df['dt_date'].max().date()
        except AttributeError:
            mr_date = pd.to_datetime(tdata_df['dt_date'], unit='ms').max().date()
        # Most recent available date to get data from
        mr_avail_date = DerivativesHelper.which_fname_date()

        # If there is new data available, get it
        if mr_date < mr_avail_date:
            print('Most recent date is less that the most recent available date')
            # Define the payload to be used
            payload = {'token': os.environ.get("iex_publish_api"),
                       'from': mr_date.strftime("%Y-%m-%d"),  # YYYY-MM-DD
                       'to': mr_avail_date.strftime("%Y-%m-%d")}  # YYYY-MM-DD
            # Get tdata from IEX cloud
            new_tdata = get_tdata(payload, base_url)
            # Combine new and old data
            tdata_df = pd.concat([tdata_df, new_tdata])
            # Reset index and drop
            tdata_df.reset_index(drop=True, inplace=True)
            # Write to local json file
            tdata_df.to_json(fname)

    else:  # If no data is saved locally, get the ytd data
        # Define the payload and range
        payload = {'token': os.environ.get("iex_publish_api"), 'range': 'ytd'}
        # Get tdata from IEX cloud
        print('local data does not exist')
        tdata_df = get_tdata(payload, base_url)
        # Write to local json file
        tdata_df.to_json(fname)

    return tdata_df
