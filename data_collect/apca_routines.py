"""Alpaca Data Collection Routines."""
# %% codecell
########################################
import os
from datetime import timedelta
from dotenv import load_dotenv
import requests
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate, help_print_arg

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
    df, fpath = None, ''

    def __init__(self):
        headers, base_url = apca_params(markets=True)
        self.create_fpath(self)
        self.get_data(self, base_url, headers)
        self.write_to_json(self)

    @classmethod
    def create_fpath(cls, self):
        """Create local fpath to write json file."""
        base_dir = baseDir().path
        fpath = f"{base_dir}/tickers/apca_ref.gz"
        self.fpath = fpath

    @classmethod
    def get_data(cls, self, base_url, headers):
        """Get Alpaca symbol reference data."""
        url = f"{base_url}/assets"
        get_ref = requests.get(url, headers=headers)

        if get_ref.status_code < 400:
            df = pd.DataFrame(get_ref.json())
            self.df = dataTypes(df).df
        else:
            help_print_arg(get_ref.content)

    @classmethod
    def write_to_json(cls, self):
        """Write dataframe to local json file."""
        self.df.to_json(self.fpath, compression='gzip')


# %% codecell
########################################

class ApcaHist():
    """Use Alpaca API to get historical data."""
    current_day = None
    df, fpath = None, ''

    def __init__(self, sym, current_day=True, ytd=False, testing=False):
        self.current_day, self.testing = current_day, testing
        self.construct_fpath(self, sym)
        # Construct parameters for request
        headers, url, params = self.construct_params(self, sym)
        # Use params to get data from Alpaca. Default is today's data
        self.get_data(self, headers, url, params)
        # If file exists, concat, otherwise just clean
        self.clean_concat_data(self)
        # Write to local json file
        self.write_to_json(self)

    @classmethod
    def construct_fpath(cls, self, sym):
        """Construct local fpath to store data."""
        dt = getDate.query('iex_eod')
        fpath_base = f"{baseDir().path}/apca/apca_hist/{dt.year}"
        fpath = f"{fpath_base}/{sym.lower()[0]}/_{sym}.gz"

        self.fpath = fpath

    @classmethod
    def construct_params(cls, self, sym):
        """Construct parameters for get request."""
        params = None
        headers, base_url = apca_params(markets=False)
        url = f"{base_url}/stocks/{sym.upper()}/bars"

        if self.current_day:
            # Get today, yesterday date in rfc format
            today = getDate.query('iex_eod')
            yesterday = (today - timedelta(days=1))

            today_rfc = getDate.date_to_rfc(today)
            yest_rfc = getDate.date_to_rfc(yesterday)

            params = ({'start': yest_rfc, 'end': today_rfc,
                       'limit': 10, 'timeframe': '1Day'})

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
                help_print_arg(df.dtypes)
            self.df = df
        else:
            help_print_arg(get.content)
            if self.testing:
                help_print_arg(f"{url} {params} {headers}")


    @classmethod
    def clean_concat_data(cls, self):
        """Clean, concat data and prepare for compression."""
        df_old, df = None, None
        if os.path.isfile(self.fpath):
            df_old = pd.read_json(self.fpath, compression='gzip')
            df = pd.concat([df_old, self.df])
            # Minimize data size
            self.df = dataTypes(df).df.copy(deep=True)

    @classmethod
    def write_to_json(cls, self):
        """Write dataframe to local json file."""
        self.df.drop_duplicates(subset='date', inplace=True)
        self.df.reset_index(drop=True, inplace=True)

        self.df.to_json(self.fpath, compression='gzip')
