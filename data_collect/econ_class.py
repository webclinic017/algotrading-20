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

from yahoofinancials import YahooFinancials

try:
    from scripts.dev.data_collect.options import DerivativeExpirations, DerivativesHelper
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate
    from scripts.dev.file_storage import fileOps, blockPrinting

except ModuleNotFoundError:
    from data_collect.options import DerivativesHelper, DerivativesStats
    from file_storage import fileOps, blockPrinting
    from multiuse.help_class import baseDir, dataTypes, getDate

    importlib.reload(sys.modules['data_collect.options'])
    importlib.reload(sys.modules['file_storage'])
    importlib.reload(sys.modules['multiuse.help_class'])

    from data_collect.options import DerivativesHelper, DerivativesStats
    from file_storage import fileOps, blockPrinting
    from multiuse.help_class import baseDir, dataTypes, getDate

# %% codecell
############################################################
"""
Get the last year of 3-month T bill rates (risk free).
Write function to find the most recent date and then get all data in
between today and that day.
"""

# %% codecell
############################################################

class yahooTbills():
    """Get current price for US T-bills - 3 month, 5 yr, 10 yr, 30 yr."""
    # 4 am to 8 pm, every hour
    tickers = ['^IRX', '^FVX', '^TNX', '^TYX']
    cols = ['3mo', '5yr', '10yr', '30yr', 'time']

    def __init__(self):
        self.get_path(self)
        self.df = self.get_data(self)
        self.write_to_json(self)

    def get_path(cls, self):
        """Get local fpath."""
        self.fpath = f"{baseDir().path}/economic_data/treasuries.gz"
        # Create an empty data frame with column names
        df = pd.DataFrame(columns=self.cols)
        # Check if local data frame already exists
        if os.path.isfile(self.fpath):
            df = pd.read_json(self.fpath, compression='gzip')
        # Return data frame
        self.df = df

    def get_data(cls, self):
        """Get data from yahoo finance."""
        treasuries = YahooFinancials(self.tickers)
        tdata = treasuries.get_current_price()

        # Add current timestamp
        tdata['time'] = datetime.datetime.now()
        # Append data to existing data frame
        df = self.df.append(tdata, ignore_index=True)

        # Remove time from columns for data conversion
        try:
            self.cols.remove('time')
        except ValueError:
            pass
        # Convert cols to float 16s
        df[self.cols] = df[self.cols].astype(np.float16)
        df.reset_index(inplace=True, drop=True)

        return df

    def write_to_json(cls, self):
        """Write data to local json file."""
        self.df.to_json(self.fpath, compression='gzip')

# %% codecell
############################################################


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
