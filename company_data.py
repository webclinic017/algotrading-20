"""
The purpose of this file is to test IEX cloud's API for key company data
"""
# %%  codecell
######################################################################################
import os

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

import datetime
from datetime import timedelta, date

from nested_lookup import nested_lookup

# %% codecell
######################################################################################
load_dotenv()  # Load environ variables
base_url = os.environ.get("base_url")

# %% codecell
######################################################################################

symbols = ["AAPL", "AMZN"]


class CompanyStats():
    """Static methods for various company data."""
    def stats_dict():
        """Create dict of url and local fpaths."""
        stats_dict = {
            'advanced_stats': {'url_suffix': 'advanced-stats',
                               'local_fpath': 'key_stats'},
            'fund_ownership': {'url_suffix': 'fund-ownership',
                               'local_fpath': 'fund_ownership'},
            'insiders': {'url_suffix': 'insider-roster',
                         'local_fpath': 'insiders'},
            'insider_trans': {'url_suffix': 'insider-transactions',
                              'local_fpath': 'insider_trans'},
            'institutional_owners': {'url_suffix': 'institutional-ownership',
                                     'local_fpath': 'institutional_owners'},
            'peers': {'url_suffix': 'peers',
                      'local_fpath': 'peers'},
        }
        return stats_dict


    @staticmethod
    def get_data(symbols, which):
        """Base function for getting company stats data."""
        load_dotenv()
        stats_dict = CompanyStats.stats_dict()
        base_url = os.environ.get("base_url")

        for sym in symbols:
            payload = {'token': os.environ.get("iex_publish_api")}
            rep = requests.get(
                f"{base_url}/stock/{sym}/{stats_dict[which]['url_suffix']}",
                params=payload
                )
            # print(rep.text)

            base_dir = f"{Path(os.getcwd()).parents[0]}/data/company_stats/{stats_dict[which]['local_fpath']}"

            Path(f"{base_dir}/{sym[0].lower()}/_{sym}").write_bytes(rep.content)


# %% codecell
######################################################################################

CompanyStats.fund_ownership(symbols)
CompanyStats.insiders(symbols)
CompanyStats.insider_trans(symbols)
CompanyStats.institutional_owners(symbols)
CompanyStats.get_data(symbols, 'peers')
# %% codecell
######################################################################################

# Get dates for the next 7 fridays and try to get data for them:

# 2nd Friday in June, September
# 3rd friday in January for this next year, year after

class Derivatives():
    """Expriations dates."""

    def __init__(self):
        """Calculate dates."""
        date_list = []
        base_friday = self.closest_friday()
        date_list = self.next_7_fridays(base_friday, date_list)
        date_list = self.find_third_friday(self, 6, date_list)
        date_list = self.find_third_friday(self, 9, date_list)
        date_list = self.january_friday(self, date_list)
        date_list = self.format_dates(date_list)
        self.date_list = date_list

    @classmethod
    def closest_friday(cls):
        """Calculate the closest friday in datetime.date."""
        for d in range(1, 5):
            if (date.today() + timedelta(days=d)).weekday() == 4:
                base_friday = date.today() + timedelta(days=d)
        # print(base_friday)
        return base_friday

    @classmethod
    def next_7_fridays(cls, base_friday, date_list):
        """Get datetime.date for next 7 fridays."""
        for d in range(1, 7):  # Get the next 7 Fridays
            date_list.append(base_friday + timedelta(weeks=d))
        return date_list

    @classmethod
    def find_third_friday(cls, self, month, date_list):
        """Get the third friday of that month."""
        # Month is an integer
        date_list.append(self.third_friday(date.today().year, month))
        return date_list

    @classmethod
    def january_friday(cls, self, date_list):
        """Get January expiration."""
        date_list.append(self.third_friday((date.today() + timedelta(weeks=52)).year, 1))
        date_list.append(self.third_friday((date.today() + timedelta(weeks=104)).year, 1))
        return date_list

    @classmethod
    def format_dates(cls, date_list):
        """Convert dates to strings."""
        date_list = [d.strftime("%Y%m%d") for d in date_list]
        # print(date_list)
        return date_list


    @classmethod
    def third_friday(cls, year, month):
        """Return datetime.date for monthly option expiration given year and
        month
        """
        # The 15th is the lowest third day in the month
        third = datetime.date(year, month, 15)
        # What day of the week is the 15th?
        w = third.weekday()
        # Friday is weekday 4
        if w != 4:
            # Replace just the day (of month)
            third = third.replace(day=(15 + (4 - w) % 7))
        return third

# %% codecell
#######################################################################

derivatives = Derivatives()

derivatives.date_list


# %% codecell
#######################################################################

def get_all_symbols():
    """Get all callable symbols."""
    payload = {'token': os.environ.get("iex_publish_api")}
    token = os.environ.get("iex_publish_api")
    rep = requests.get(
        f"https://cloud.iexapis.com/beta/ref-data/symbols?token={token}"
        )
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/tickers"
    Path(f"{base_dir}/all_symbols").write_bytes(rep.content)

# %% codecell
#######################################################################

payload = {'token': os.environ.get("iex_publish_api")}
rep = requests.get(
    f"{base_url}/stock/market/collection/sector?collectionName=Technology",
    params=payload
    )

# %% codecell
#######################################################################

base_dir = f"{Path(os.getcwd()).parents[0]}/data/tickers"
Path(f"{base_dir}/technology").write_bytes(rep.content)


# %% codecell
#######################################################################
def get_otc_symbols():
    """Get all OTC symbols."""
    payload = {'token': os.environ.get("iex_publish_api")}
    token = os.environ.get("iex_publish_api")
    rep = requests.get(
        f"https://cloud.iexapis.com/beta/ref-data/otc/symbols?token={token}"
        )
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/tickers"
    Path(f"{base_dir}/otc").write_bytes(rep.content)

get_otc_symbols()

# %% codecell
#######################################################################

def get_sector_lists():
    """Get a list of all sectors/collections."""
    payload = {'token': os.environ.get("iex_publish_api")}
    token = os.environ.get("iex_publish_api")
    rep = requests.get(
        f"https://cloud.iexapis.com/beta/ref-data/sectors?token={token}"
        )
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/tickers"
    Path(f"{base_dir}/sector_lists").write_bytes(rep.content)

get_sector_lists()


# %% codecell
#######################################################################


def get_tags():
    """Get available tags."""
    payload = {'token': os.environ.get("iex_publish_api")}
    token = os.environ.get("iex_publish_api")
    rep = requests.get(
        f"https://cloud.iexapis.com/beta/ref-data/tags?token={token}"
        )
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/tickers"
    Path(f"{base_dir}/tags").write_bytes(rep.content)

get_tags()
