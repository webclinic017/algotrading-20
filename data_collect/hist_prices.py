"""Get historical prices from IEX cloud."""
# %% codecell
############################################
import os.path
from datetime import date
from dotenv import load_dotenv

import pandas as pd
import requests

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, dataTypes
    # from app.tasks_test import print_arg_test
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, dataTypes

# %% codecell
############################################


class HistPricesV2():
    """Second version of getting IEX hist prices."""

    base_dir = baseDir().path
    base_path = f"{base_dir}/StockEOD"
    is_file, df, need_ytd = False, False, False
    need_data = True
    dts_need = []

    def __init__(self, sym):
        self.check_existing(self, sym)
        # If existing data is current, do nothing. Else get data
        if self.need_data:
            self.get_iex_params(self, sym)

            if self.need_ytd:  # If ytd data needed
                self.get_ytd(self)
            else:  # If exact dates needed
                self.get_exact_dates(self)

            # Write dataframe to json file
            # self.write_to_json(self)

    @classmethod
    def check_existing(cls, self, sym):
        """Determine parameters to use."""
        dt = getDate.query('iex_eod')
        fpath = f"{self.base_path}/{dt.year}/{sym.lower()[0]}/_{sym}.gz"

        if os.path.isfile(fpath):
            self.get_dates_or_ytd(self, fpath, dt)
        else:  # If file does not exists, get ytd
            self.is_file = False
            self.need_ytd = True

        self.fpath = fpath

    @classmethod
    def get_iex_params(cls, self, sym):
        """Get parameters used for iex calls."""
        load_dotenv()
        true = True
        base_url = os.environ.get("base_url")
        self.url = f"{base_url}/stock/{sym}/chart"
        self.payload = ({'token': os.environ.get("iex_publish_api"),
                         'includeToday': true, 'chartByDay': true})

    @classmethod
    def get_dates_or_ytd(cls, self, fpath, dt):
        """Get the exact dates needed or determine full ytd need."""
        # Read local json file
        df = pd.read_json(fpath, compression='gzip')

        # If for some reason the df is empty, or data has an error
        if df.empty:
            os.remove(fpath)
            self.need_ytd = True
        # If the max date is equal to todays date, end sequence
        elif df['date'].max() == dt:
            self.need_data = False
            self.df = df
        # Assuming df exists and is valid
        else:
            # Make a pandas datetime range
            # bd_range = pd.bdate_range(date(dt.year, 1, 2), dt)
            bd_range = getDate.get_bus_days()
            times_need = bd_range['date'][~bd_range['date'].isin(df['date'])]
            dts_need = [bd.date().strftime('%Y%m%d') for bd in times_need]

            # If more than 10 dates are needed, just get YTD
            if len(dts_need) > 10:
                self.need_ytd = True
            else:
                self.dts_need = dts_need
            # Assign dataframe to self.df
            self.df = df

    @classmethod
    def get_ytd(cls, self):
        """Get YTD data."""
        self.payload['range'] = 'ytd'
        get_errors = []
        get = requests.get(self.url, params=self.payload)
        if get.status_code == 200:
            df = pd.DataFrame(get.json())
            # self.df = dataTypes(df).df
            mod_df = dataTypes(df).df
            # Write dataframe to json file
            mod_df.to_json(self.fpath, compression='gzip')
            # self.write_to_json(self)
        else:
            get_errors.append(f"Error with {self.url}. {get.content}")
        # Print out any errors that may have arisen.
        self.get = get
        self.class_print(get_errors)

    @classmethod
    def get_exact_dates(cls, self):
        """Get exact dates."""
        self.payload['range'], df_list = 'date', []
        get_errors = []
        # For all the dates needed
        for fdt in self.dts_need:
            self.payload['exactDate'] = fdt
            get = requests.get(self.url, params=self.payload)

            if get.status_code == 200:
                df_list.append(pd.DataFrame(get.json()))
            else:
                get_errors.append(f"Error with {self.url}. {get.content}")

        # Print out any errors that may have arisen.
        if len(get_errors) > 1:
            self.class_print(get_errors)

        # Concat all new dates if list is > 1
        if len(df_list) > 0:
            new_df = pd.concat(df_list)
            # Concat existing df with new dates
            all_df = pd.concat([self.df, new_df])
            all_df.drop_duplicates(subset=['date'], inplace=True)
            all_df.reset_index(drop=True, inplace=True)

            mod_df = dataTypes(all_df).df
            # Write dataframe to json file
            mod_df.to_json(self.fpath, compression='gzip')
            # self.df = dataTypes(all_df).df.copy(deep=True)
            # Write dataframe to json file
            # self.write_to_json(self)

    @classmethod
    def write_to_json(cls, self):
        """Write dataframe to local file."""
        self.df.to_json(self.fpath, compression='gzip')

    @classmethod
    def class_print(cls, arg):
        """Print argument that's passed for local/server environs."""
        try:
            print_arg_test.delay(arg)
        except NameError:
            print(arg)
