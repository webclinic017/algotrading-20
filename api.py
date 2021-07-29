"""
Get data from server through API.
"""
# %% codecell
####################################
import json
from json import JSONDecodeError
from io import BytesIO

import pandas as pd
import numpy as np
import requests

try:
    from scripts.dev.multiuse.help_class import getDate
except ModuleNotFoundError:
    from multiuse.help_class import getDate

# %% codecell
####################################


def make_url_dict():
    """Make the url dict."""
    url_dict = ({
        'treasuries': '/econ/treasuries',
        'cboe_mmo_raw': '/cboe/mmo/raw',
        'cboe_mmo_top': '/cboe/mmo/top',
        'cboe_mmo_syms': '/cboe/mmo/syms',
        'cboe_mmo_exp_all': '/cboe/mmo/explore/all',
        'cboe_mmo_exp_last': '/cboe/mmo/explore/last',
        'st_stream': '/stocktwits/user_stream',
        'st_trend_all': '/stocktwits/trending/all',
        'st_trend_today': '/stocktwits/trending/today/explore',
        'st_watch': '/stocktwits/watchlist',
        'iex_quotes_raw': '/prices/eod/all',
        'iex_comb_today': f"/prices/combined/{getDate.query('cboe')}",
        'new_syms_today': '/symbols/new/today',
        'new_syms_all': '/symbols/new/all',
        'stock_data': '/symbols/data',
        'all_symbols': '/symbols/all',
        'otc_syms': 'symbols/otc',
        'cs_top_vol': '/scans/vol/avg',
        'sec_ref': '/sec/data/ref',
        'sec_inst_holdings': '/sec/data/institutions',
        'sec_master_mr': '',
        'sec_master_all': '/sec/data/master_idx/all/False',
        'redo': ''
    })

    return url_dict


class serverAPI():
    """Methods for server API endpoints."""

    url, df = None, None
    base_url = "https://algotrading.ventures/api/v1"
    url_dict = make_url_dict()

    # Data to conacatenate
    concat = ['st_trend', 'cboe_mmo_top']

    def __init__(self, which, **kwargs):
        self.check_params(self, which, **kwargs)
        df = self.get_data(self, which)

        if df.shape[0] == 0:
            print(self.url)
        self.df = df

    @classmethod
    def check_params(cls, self, which, **kwargs):
        """Check passed parameters and urls."""
        refresh, val = None, None
        if which in ('sec_master_mr'):
            if 'refresh' in kwargs.keys():
                refresh = kwargs['refresh']
            if 'val' in kwargs.keys():
                val = kwargs['val']

            self.url_dict[which] = f"/sec/data/master_idx/{val}/{refresh}"
        elif which == 'redo' and 'val' in kwargs.keys():
            val = kwargs['val']
            self.url_dict[which] = f"/redo/functions/{val}"
        elif which == 'stock_data' and 'symbol' in kwargs.keys():
            symbol = kwargs['symbol']
            self.url_dict[which] = f"{self.url_dict[which]}/{symbol}"

    @classmethod
    def get_data(cls, self, which):
        """Get data from server."""
        df, get_json = None, None
        url = f"{self.base_url}{self.url_dict[which]}"
        get = requests.get(url)
        try:
            get_json = json.load(BytesIO(get.content))
        except JSONDecodeError:
            get_json = get.json()

        df = get_json

        # If data type needs to be looped/concatenated
        if which in self.concat:
            df = self.concat_data(self, df)
            # Clean stocktwits trending data
            if which == 'st_trend':
                df = self._clean_st_trend(self, df)
        elif which in ('cboe_mmo_exp_all', 'cboe_mmo_exp_last'):
            df = self._mmo_explore_all(self, df)
        elif which == 'stock_data':
            df = pd.read_json(df['iex_hist']).copy(deep=True)
        else:
            # Convert to dataframe
            df = pd.DataFrame(df)

        return df

    @classmethod
    def concat_data(cls, self, dict):
        """Loop through and concatenate dataframes."""

        # Convert all keys to dataframes
        for key in dict.keys():
            dict[key] = pd.DataFrame(dict[key])

        # Get a flattened list of dataframes
        items = list(dict.values())
        # Concatenate dataframes
        this_df = pd.concat(items)
        this_df.reset_index(inplace=True, drop=True)

        return this_df

        """
        all_df = pd.DataFrame()

        # Loop through dictionary and append data to dataframe
        for x in dict:
            # dict[x]['dataDate'] = str(x)[-13:-3]
            new_df = pd.DataFrame(dict[x])
            all_df = pd.concat([all_df, new_df])
            # all_df = all_df.append(dict[x], ignore_index=True)
            # all_df = pd.DataFrame.from_dict(all_df)
        all_df.reset_index(inplace=True, drop=True)

        return all_df
        """

    @classmethod
    def _clean_st_trend(cls, self, df):
        """Clean and return st_trending data."""
        df.dropna(axis=0, inplace=True)
        # Convert to timestamp and get date and hour
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour

        # Convert columns to relevant data type
        df['hour'] = df['hour'].astype(np.uint8)
        df['id'] = df['id'].astype(np.uint16)
        df['symbol'] = df['symbol'].astype('category')

        df['watchlist_count'] = df['watchlist_count'].astype(np.uint32)
        df.rename(columns={'watchlist_count': 'wCount'}, inplace=True)

        return df

    @classmethod
    def _mmo_explore_all(cls, self, df):
        """Return dict of processed/converted dataframes for each CBOE day."""
        cboe_dict = {}

        for key in df.keys():
            key_mod = key[-13:-3]
            try:
                if cboe_dict[key_mod].shape[0] > 1:
                    key_df = pd.DataFrame(df[key])
                    cboe_dict[key_mod] = pd.concat([cboe_dict[key_mod], key_df])
            except KeyError:
                cboe_dict[key_mod] = pd.DataFrame(df[key])

        for key in cboe_dict.keys():
            cboe_dict[key]['Cboe ADV'] = cboe_dict[key]['Cboe ADV'].where(cboe_dict[key]['Cboe ADV'] != 0, 1)
            cboe_dict[key]['vol/avg'] = (cboe_dict[key]['totVol'] / cboe_dict[key]['Cboe ADV']).astype(np.float16)
            cboe_dict[key] = cboe_dict[key].sort_values(by=['vol/avg', 'totVol'], ascending=False).head(150)
            cboe_dict[key].reset_index(inplace=True, drop=True)

        return cboe_dict










# %% codecell
####################################
