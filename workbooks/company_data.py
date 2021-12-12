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
import glob
from tqdm import tqdm

import importlib
import sys
import datetime
from datetime import timedelta, date

from multiuse.help_class import baseDir, getDate, write_to_parquet
#from .iex_class import urlData
from data_collect.iex_class import urlData
importlib.reload(sys.modules['data_collect.iex_class'])
from data_collect.iex_class import urlData

from api import serverAPI

# %% codecell
######################################################################################


def get_company_meta_data():
    """Get company meta data, save locally, from IEX."""
    all_symbols = serverAPI('all_symbols').df
    all_cs = all_symbols[all_symbols['type'].isin(['cs', 'ad'])]
    sym_list = all_cs['symbol'].unique().tolist()

    bpath = Path(baseDir().path, 'company_stats/meta')


    for sym in tqdm(sym_list):
        try:
            ud = urlData(f"/stock/{sym}/company")
            fpath_suf = f"{sym.lower()[0]}/_{sym}.parquet"
            fpath = bpath.joinpath(fpath_suf)
            write_to_parquet(ud.df, fpath)
        except Exception as e:
            print(f"Company meta stats error: {type(e)} {str(e)}")

get_company_meta_data()

# %% codecell

fund_class = companyStats(symbols, 'fund_ownership')
fund_df = fund_class.df.copy(deep=True)


# %% codecell
######################################################################################

symbols = ["AAPL", "AMZN"]


class companyStats():
    """Static methods for various company data."""

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

    def __init__(self, symbols, which):
        self.get_fname(self, which)
        self.df = self.check_local(self, symbols, which)

    @classmethod
    def get_fname(cls, self, which):
        """Get local fname to use."""
        base_dir = f"{baseDir().path}/company_stats"
        self.fpath = f"{base_dir}/{self.stats_dict[which]['local_fpath']}"

    @classmethod
    def check_local(cls, self, symbols, which):
        """Check for local data before requesting from IEX."""
        which = 'fund_ownership'
        base_dir = f"{baseDir().path}/company_stats"
        all_df = pd.DataFrame()

        for sym in symbols:
            full_path = f"{base_dir}/{self.stats_dict[which]['local_fpath']}/{sym[0].lower()}/*"
            data_today = f"{full_path}_{date.today()}"
            data_yest = f"{full_path}_{date.today() - timedelta(days=1)}"

            if os.path.isfile(data_today):
                df = pd.read_json(data_today, compression='gzip')
                all_df = pd.concat([all_df, df])
            elif os.path.isfile(data_yest):
                df = pd.read_json(data_yest, compression='gzip')
                all_df = pd.concat([all_df, df])
            else:
                df = self._get_data(self, sym, which)
                all_df = pd.concat([all_df, df])

        all_df.reset_index(inplace=True, drop=True)
        return all_df

    @classmethod
    def _get_data(cls, self, sym, which):
        """Base function for getting company stats data."""
        url = f"/stock/{sym}/{self.stats_dict[which]['url_suffix']}"
        df = urlData(url).df
        path = Path(f"{self.fpath}/{sym[0].lower()}/_{sym}_{date.today()}.gz")
        df.to_json(path, compression='gzip')
        return df


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
