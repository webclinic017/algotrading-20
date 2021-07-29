"""Alpaca Data Collection Routines."""
# %% codecell
########################################
import os
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
