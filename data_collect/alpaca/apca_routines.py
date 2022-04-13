"""Alpaca Data Collection Routines.
** Deprecated - all methods here are assumed to be outdated **
"""
# %% codecell
########################################
import os
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path
import requests
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg, write_to_parquet
    # from scripts.dev.multiuse.pathClasses.construct_paths import PathConstructs
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg, write_to_parquet
    # from multiuse.pathClasses.construct_paths import PathConstructs

# %% codecell
########################################


def apca_params(markets=False):
    """Get alpaca headers to use in auth request."""
    load_dotenv()
    base_url = ''

    apca_client = os.environ.get("alpaca_client_id")
    apca_secret = os.environ.get("alpaca_client_secret")

    headers = ({'APCA-API-KEY-ID': apca_client,
                'APCA-API-SECRET-KEY': apca_secret})

    if not markets:
        base_url = 'https://data.alpaca.markets/v2'
    else:
        base_url = 'https://api.alpaca.markets/v2'

    return headers, base_url

# %% codecell
########################################


class ApcaSymbols():
    """Get Alpaca assets (symbols)."""

    def __init__(self):
        headers, base_url = apca_params(markets=True)
        self.fpath = self.create_fpath(self)
        self.df = self.get_data(self, base_url, headers)
        self._write_to_parquet(self)

    @classmethod
    def create_fpath(cls, self):
        """Create local fpath to write parquet file."""
        bpath = Path(baseDir().path, 'tickers', 'symbol_list')
        return bpath.joinpath('apca_ref.parquet')

    @classmethod
    def get_data(cls, self, base_url, headers):
        """Get Alpaca symbol reference data."""
        url = f"{base_url}/assets"
        get = requests.get(url, headers=headers)

        if get.status_code < 400:
            df = pd.DataFrame(get.json())
            return df
        else:
            help_print_arg(get.content)
            return df

    @classmethod
    def _write_to_parquet(cls, self):
        """Write dataframe to local parquet file."""
        if isinstance(self.df, pd.DataFrame):
            write_to_parquet(self.df, self.fpath)


# %% codecell
########################################


class ApcaHist():
    """Use Alpaca API to get historical data."""

    bpath = Path(baseDir().path, 'histStocks', 'apca', 'apca_hist')

    def __init__(self, sym, **kwargs):
        self.assign_variables(self, **kwargs)
        self.fpath = self.construct_fpath(self, sym)
        # Construct parameters for request
        headers, url, params = self.construct_params(self, sym)
        # Use params to get data from Alpaca. Default is today's data
        self.get_data(self, headers, url, params)
        # Write to local parquet file
        if isinstance(self.df, pd.DataFrame):
            self.write_to_parquet(self)
        else:
            help_print_arg(f"Data Collection for symbol {sym} failed")

    @classmethod
    def assign_variables(cls, self, **kwargs):
        """Assign relevant variables to class."""
        self.current_day = kwargs.get('current_day', False)
        self.ytd = kwargs.get('ytd', False)
        self.testing = kwargs.get('testing', False)

    @classmethod
    def construct_fpath(cls, self, sym):
        """Construct local fpath to store data."""
        # fpath = PathConstructs.hist_sym_fpath(sym, self.bpath)
        dt = getDate.query('iex_close')
        fsuf = f"{sym.lower()[0]}/_{sym}.parquet"
        fpath = self.bpath.joinpath(str(dt.year), fsuf)
        return fpath

    @classmethod
    def construct_params(cls, self, sym):
        """Construct parameters for get request."""
        params = None
        headers, base_url = apca_params(markets=False)
        url = f"{base_url}/stocks/{sym.upper()}/bars"

        # End date would be today regardless of ytd or today's hist
        today = getDate.query('iex_eod')
        # Convert that to rfc-3339 format
        today_rfc = getDate.date_to_rfc(today)

        if self.current_day:
            # Get yesterday date in rfc format
            yesterday = (today - timedelta(days=1))
            yest_rfc = getDate.date_to_rfc(yesterday)

            params = ({'start': yest_rfc, 'end': today_rfc,
                       'limit': 10, 'timeframe': '1Day'})
        elif self.ytd:
            bus_days = getDate().get_bus_days(this_year=True)
            first_day = bus_days.min()['date'].date()
            first_day_rfc = getDate.date_to_rfc(first_day)

            params = ({'start': first_day_rfc, 'end': today_rfc,
                       'limit': 10000, 'timeframe': '1Day'})

        return headers, url, params

    @classmethod
    def get_data(cls, self, headers, url, params):
        """Use variables to get hist data from Alpaca."""
        get = requests.get(url, headers=headers, params=params)

        if get.status_code < 400:
            df = pd.DataFrame(get.json()['bars'])
            try:
                df['date'] = getDate.rfc_to_date(df['t'])
            except KeyError:
                help_print_arg(str(df.dtypes))
            self.df = df
        else:
            help_print_arg(get.content)
            if self.testing:
                help_print_arg(f"{url} {params} {headers}")

    @classmethod
    def write_to_parquet(cls, self):
        """Write dataframe to local parquet file."""
        kwargs = {'cols_to_drop': ['date']}
        write_to_parquet(self.df, self.fpath, **kwargs)
