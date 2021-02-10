import os
import os.path
from os import path
import sys
from pathlib import Path
from datetime import datetime, date
import pytz
import json

from dotenv import load_dotenv

import pandas as pd
import numpy as np
import requests
from json import JSONDecodeError


class fileOps():
    """Helper class for reading/writing from local database."""

    @staticmethod
    def read_from_json(symbols, years):
        """Read from json with params and return dataframe"""
        # Get current working directory and go one level up
        base_dir = f"{Path(os.getcwd()).parents[0]}/data/StockEOD"

        # Create list of years to get data from. This year + n years historical
        current_year = datetime.today().year
        years_list = np.sort(list(range(current_year, current_year - years, -1)))

        # Create empty data frame to use later
        df = pd.DataFrame()
        # Current stores the symbol, and json market data until merge
        current = {}
        # If any new data was added. If none, set to false
        new_data = True

        # Set up cutoff time for after 4:15 pm for today's daily close market data.
        nyc_datetime = datetime.now(pytz.timezone('US/Eastern'))
        cutoff_hm = 16.15
        nyc_hm = nyc_datetime.hour + (nyc_datetime.minute * .10)

        for sym in symbols:
            try:
                # Read and append json data for that symbol, for each year
                [df := df.append(  # List comprehension
                    pd.read_json(f"{base_dir}/{y}/{sym[0]}/_{sym}")
                    )
                 for y in years_list
                ]
                current[sym] = {'current':
                                    (date.today() -        # Difference between
                                     pd.to_datetime(       # Today and the
                                         df['date'].max()  # Most recent date
                                       ).date()
                                    ).days,                # In terms of days
                                'data': ''}                # Create empty value
                # If data is older than one day or after 4:15 pm for today
                if current[sym]['current'] not in (0, 1):
                    print(f"Your data for symbol {sym} is {current[sym]['current']} days old. Updating now...")
                    current[sym]['data'] = fileOps.get_stock_data(sym, current[sym]['current'])
                # Get today's market data after 4:15 pm
                elif (current[sym]['current'] == 1) and (nyc_hm > cutoff_hm):
                    print(f"Today's market data for symbol {sym}. Updating now...")
                    current[sym]['data'] = fileOps.get_stock_data(sym, current[sym]['current'])
                # If no new data was added
                else:
                    new_data = False
                    print(f"Data for {sym} is current.")

            except ValueError:  # No data for that symbol exists, get data
                current[sym] = {'current': 0, 'data': fileOps.all_stock_data(sym)}
                print('Cannot find symbol. New stock data being added')

        if new_data:  # If any new data was added
            # Merge new json market data with previous market data
            df = fileOps.concat_and_sort(df, current, symbols)
            # Write dataframe to local json file
            fileOps.write_to_json(df)

        return df, current

    @staticmethod
    def all_stock_data(sym):
        iex_api, url = fileOps.sandbox_or_test()
        # Getting data for a normal stock:
        date_range = 'y' # Yearly data
        timed_url = f"{url}/stock/{sym}/chart/{date_range}"

        payload = {'token': iex_api, 'range': '6y'}
        rep = requests.get(timed_url, params=payload)
        try:
            rep = rep.json()
        except JSONDecodeError:
            print('We made it to the error of decoding json')
            print(rep.text)
            return False

        return rep

    @staticmethod
    def get_stock_data(sym, n_days):
        iex_api, url = fileOps.sandbox_or_test()
        # Getting data for a normal stock:
        date_range = 'ytd'

        timed_url = f"{url}/stock/{sym}/chart/{date_range}"

        payload = {'token': iex_api, 'last': n_days}
        rep = requests.get(timed_url, params=payload)
        try:
            rep = rep.json()
        except JSONDecodeError:
            print('Could not decode json. Are you in sandbox mode?')
            print(rep.text)
            print(rep)
            print(timed_url)
            print(payload)
            return False

        return rep

    @staticmethod
    def sandbox_or_test():
        """Load url and api keys."""
        load_dotenv()  # Load environ variables

        if os.environ.get("environment") == 'sandbox':
            iex_api = os.environ.get("iex_sandbox_api")
            url = os.environ.get("sandbox_url")
        else:
            iex_api = os.environ.get("iex_publish_api")
            url = os.environ.get("base_url")

        return iex_api, url

    @staticmethod
    def concat_and_sort(df, current, symbols):
        """Merge with original dataframe and sort."""
        df_current = pd.DataFrame()

        for sym in symbols:
            try:
                df_current = pd.concat([df_current, pd.DataFrame(current[sym]['data'])])
            except ValueError:
                df_current = pd.DataFrame(current[sym]['data'])
                print('Problem merging new json data of different symbols')

        try:
            df_current['date'] = pd.to_datetime(df_current['date'], unit='ns')
        except KeyError:
            print(df_current)

        try:
            df = pd.concat([df[[c for c in df_current.columns]], df_current]).sort_values(by='date', ascending=True)
        except ValueError:
            df = df_current
            print('Value Error. Assuming new symbol being added to local filesystem')
        except KeyError:
            df = df_current
            print('Key Error. Assuming new symbol being added to local filesystem')

        return df

    @staticmethod
    def write_to_json(df):
        """Write dataframes to local JSON files by year"""
        base_dir = f"{Path(os.getcwd()).parents[0]}/data/StockEOD"

        try:
            # Get a rounded list of all years
            years = np.around(np.sort(df['year'].value_counts().index.values), 0)
        except KeyError:  # If no year column create one
            # Convert date column into datetime
            df['date_as_dt'] = pd.to_datetime(df['date'])
            df['year'] = df['date_as_dt'].dt.year
            # Get a rounded list of all years
            years = np.around(np.sort(df['year'].value_counts().index.values), 0)

        # Get a list of all unique symbols being used
        symbols = np.sort(df['symbol'].value_counts().index.values)

        for sym in symbols:
            for y in years:
                y = int(y)
                fname = f"{base_dir}/{y}/{sym[0].lower()}/_{sym}"

                """
                # Remove existing file
                try:
                    os.remove(fname)
                    # Checking that file was removed
                    # print(path.exists(fname))
                except FileNotFoundError:
                    pass
                """

                try:
                    # Reset index and drop, inplace
                    df.reset_index(drop=True, inplace=True)
                    df.loc[(df['year'] == y) & (df['symbol'] == sym)].to_json(fname)
                except FileNotFoundError:
                    print(f"Data for the year {y} of symbol {sym} could not be gathered")
