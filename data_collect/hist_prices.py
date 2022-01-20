"""Get historical prices from IEX cloud."""
# %% codecell
############################################
import os.path
from dotenv import load_dotenv

import pandas as pd
import requests

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
    from app.tasks_test import print_arg_test
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet

# %% codecell
############################################


class HistPricesV2():
    """Second version of getting IEX hist prices."""

    base_dir = baseDir().path
    base_path = f"{base_dir}/StockEOD"
    is_file, df, need_ytd = False, False, False
    need_data = True
    dts_need = []

    def __init__(self, sym, testing=False, last_month=False, previous=False):
        self.testing = testing
        self.last_month, self.previous = last_month, previous
        # Check if no data path exists - default get ytd
        self.check_existing(self, sym)
        self.get_iex_params(self, sym)

        if last_month or previous:
            self.get_last_range(self, sym)
        elif self.need_data:
            if self.need_ytd:  # If ytd data needed
                self.get_ytd(self)
            else:  # If exact dates needed
                self.get_exact_dates(self)
        else:
            msg = 'HistPricesV2: None of the __init__ conditions satisfied'
            help_print_arg(msg)
            raise NameError

    @classmethod
    def check_existing(cls, self, sym):
        """Determine parameters to use."""
        dt = getDate.query('iex_eod')
        fpath = f"{self.base_path}/{dt.year}/{sym.lower()[0]}/_{sym}.parquet"

        if os.path.isfile(fpath) and not self.previous and not self.last_month:
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
        if self.previous:
            self.url = f"{base_url}/stock/{sym}/previous"
            self.payload = {'token': os.environ.get("iex_publish_api")}
        else:
            self.url = f"{base_url}/stock/{sym}/chart"
            self.payload = ({'token': os.environ.get("iex_publish_api"),
                             'includeToday': true, 'chartByDay': true})
        # Update range param for lat_month
        if self.last_month:
            self.payload['range'] = '1m'

    @classmethod
    def get_last_range(cls, self, sym):
        """Get last month of data."""
        get = requests.get(self.url, params=self.payload)
        # If at first you don't succeed, try, try again.
        if get.status_code != 200:
            get = requests.get(self.url, params=self.payload)
        self.get = get

        if get.status_code == 200:
            try:
                df = pd.DataFrame(get.json())
            except ValueError:
                df = pd.DataFrame.from_dict(get.json(), orient='index').T
            # self.df = dataTypes(df).df
            if os.path.isfile(self.fpath):
                old_df = pd.read_parquet(self.fpath)
                df_all = pd.concat([old_df, df]).reset_index(drop=True)
                write_to_parquet(df_all, self.fpath)
                # Assign dataframe to class attribute
                self.df = df_all
            else:
                # Write dataframe to parquet file
                write_to_parquet(df, self.fpath)
                # Assign dataframe to class attribute
                self.df = df
        else:
            msg = f"IexHistV2 for {sym} get request failed with status_code {get.status_code}"
            help_print_arg(msg)

    @classmethod
    def get_dates_or_ytd(cls, self, fpath, dt):
        """Get the exact dates needed or determine full ytd need."""
        # Read local parquet file
        df = pd.read_parquet(fpath)

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
            bd_range = getDate.get_bus_days(testing=False, this_year=True)
            bd_range = bd_range[bd_range['date'].dt.date <= dt].copy(deep=True)
            times_need = bd_range['date'][~bd_range['date'].isin(df['date'])]
            dts_need = [bd.date().strftime('%Y%m%d') for bd in times_need]

            if self.testing:
                self.class_print(dts_need)

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
            write_to_parquet(df, self.fpath)
        else:
            get_errors.append(f"Error with {self.url}. {get.content}")
        # Print out any errors that may have arisen.
        self.get = get

        if len(get_errors) > 0:
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

            write_to_parquet(all_df, self.fpath)

    @classmethod
    def write_to_parquet(cls, self):
        """Write dataframe to local file."""
        write_to_parquet(self.df, self.fpath)

    @classmethod
    def class_print(cls, arg):
        """Print argument that's passed for local/server environs."""
        try:
            print_arg_test.delay(arg)
        except NameError:
            print(arg)
