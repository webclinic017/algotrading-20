# % codecell

import os
import json

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

from datetime import date, timedelta
import datetime
import pytz

import glob

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet
    from scripts.dev.file_storage import fileOps
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_parquet
    from file_storage import fileOps

# %% codecell
##########################################################

class DerivativesStats():
    """Statistics for options data."""

    @staticmethod
    def put_call_ratio(df):
        """Calculate the put/call ratio."""
        # Create a new data frame to calculate the p/c ratio
        p_c_group = df.groupby(by=['expirationDate', 'strikePrice', 'side', 'id']).sum().reset_index().copy(deep=True)

        # Define the p/c ratio for the put rows
        p_c_group['p/c'] = np.where(
                        (
                            (p_c_group['expirationDate'] == p_c_group['expirationDate'].shift(-1, axis=0)) &
                            (p_c_group['strikePrice'] == p_c_group['strikePrice'].shift(-1, axis=0)) &
                            (p_c_group['side'].isin(['call'])) &
                            (p_c_group['side'].shift(-1, axis=0).isin(['put']))
                        ),
                            (p_c_group['volume'].shift(-1, axis=0) / p_c_group['volume'])
                         ,  0
                    )
        # Define the p/c ratio for the call rows
        p_c_group['p/c'] = np.where(
            (p_c_group['p/c'].shift(1, axis=0) > 0),
             p_c_group['p/c'].shift(1, axis=0), p_c_group['p/c']
        )

        # Define the p/c over open interest ratio for the put rows
        p_c_group['p/c_oi'] = np.where(
                        (
                            (p_c_group['expirationDate'] == p_c_group['expirationDate'].shift(-1, axis=0)) &
                            (p_c_group['strikePrice'] == p_c_group['strikePrice'].shift(-1, axis=0)) &
                            (p_c_group['side'].isin(['call'])) &
                            (p_c_group['side'].shift(-1, axis=0).isin(['put']))
                        ),
                            (p_c_group['openInterest'].shift(-1, axis=0) / p_c_group['openInterest'])
                         ,  0
                    )
        # Define the p/c ratio for the call rows
        p_c_group['p/c_oi'] = np.where(
            (p_c_group['p/c_oi'].shift(1, axis=0) > 0),
             p_c_group['p/c_oi'].shift(1, axis=0), p_c_group['p/c_oi']
        )
        # Drop p/c column if it already exists
        try:
            df.drop(labels=['p/c'], axis=1, inplace=True)
            df.drop(labels=['p/c_oi'], axis=1, inplace=True)
        except KeyError:
            pass
        # Merge back into the original dataframe
        df = pd.merge(df, p_c_group[['id', 'p/c', 'p/c_oi']], on='id', how='left')


        return df


class DerivativesHelper():
    """Static methods to implement options data."""

    @staticmethod
    def which_fname_date():
        """Figure out which date to use for file names."""
        nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
        nyc_hm = nyc_datetime.hour + (nyc_datetime.minute/60)
        cutoff_hm = 9.55  # Past 9:30 AM

        if nyc_hm < cutoff_hm:
            da_min = -1
        else:
            da_min = 0

        if date.today().weekday() in (0, 3):
            days = 3 - da_min
            fname_date = (date.today() - timedelta(days=days))
        elif date.today().weekday() in (1, 2, 3, 4):
            days = 1 - da_min
            fname_date = (date.today() - timedelta(days=days))
        elif date.today().weekday() == 5:
            days = 2 - da_min
            fname_date = (date.today() - timedelta(days=days))

        return fname_date

    @staticmethod
    def store_all_third_fridays():
        """Local formatted list of all fridays for next 20 years."""
        year_list = list(range(2020, 2040, 1))
        month_list = list(range(1, 13, 1))

        third_fridays_20y = []

        for y in year_list:
            for m in month_list:
                third_fridays_20y.append(DerivativesHelper.all_third_fridays(y, m))

        third_fridays_20y = [d.strftime("%Y%m%d") for d in third_fridays_20y]

        # Load base_directory (derivatives data)
        dir_all_fridays = f"{baseDir().path}/derivatives"
        fname = f"{dir_all_fridays}/all_fridays_20_years.parquet"

        write_to_parquet(third_fridays_20y, fname)

    @staticmethod
    def get_all_third_fridays():
        """Get all third fridays stored locally."""
        # Load base_directory (derivatives data)
        dir_all_fridays = f"{baseDir().path}/derivatives"
        fname = f"{dir_all_fridays}/all_fridays_20_years.parquet"

        try:
            third_fridays_20y = pd.read_parquet(fname)
        except FileNotFoundError:
            DerivativesHelper.store_all_third_fridays()
            third_fridays_20y = pd.read_parquet(fname)

        return third_fridays_20y

    @staticmethod
    def all_third_fridays(year, month):
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

    @staticmethod
    def read_der_data():
        """Read local derivative data, most recent."""
        # Load base_directory (derivatives data)
        base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/EOD_prices"

        # Get most recent date for derivatives data
        most_recent_der_date = DerivativesHelper.which_fname_date().strftime("%Y-%m-%d")

        # Get a list of all data from most recent date
        choices = glob.glob(f"{base_dir}/*/*{most_recent_der_date}")

        recent_der_df = pd.DataFrame()

        for stock_path in choices:
            recent_der_df = pd.concat([recent_der_df, pd.read_parquet(stock_path)])

        return recent_der_df


    @staticmethod
    def get_stock_data(look_closer):
        """Read local parquet or use IEX to get data."""
        # Returns df of all stock data, and one for just recent close.

        # Get a list of all symbols for unusual options activity
        unusual_symbols = look_closer['symbol'].value_counts().index.values

        # Get 6y of data for stocks, if no data exists
        df_stocks, current = fileOps.read_from_json(unusual_symbols, years=6)

        # Get a formatted recent date to loc by
        most_recent_date = DerivativesHelper.which_fname_date().strftime("%Y-%m-%d")
        # Get a dataframe of all stock values - mr = most_recent
        mr_stocks = df_stocks[df_stocks['date'].isin([most_recent_date])]

        return df_stocks, mr_stocks




class DerivativeExpirations():
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
        for d in range(1, 7):
            if (date.today() + timedelta(days=d)).weekday() == 4:
                base_friday = date.today() + timedelta(days=d)
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
