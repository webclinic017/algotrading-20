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

# %% codecell
####################################

class serverAPI():
    """Methods for server API endpoints."""
    base_url = "https://algotrading.ventures/api/v1"
    url_dict = ({
        'treasuries': '/econ/treasuries',
        'cboe_mmo_raw': '/cboe/mmo/raw',
        'cboe_mmo_top': '/cboe/mmo/top',
        'cboe_mmo_syms': '/cboe/mmo/syms',
        'st_stream': '/stocktwits/user_stream',
        'st_trend_all': '/stocktwits/trending/all',
        'st_trend_today': '/stocktwits/trending/today/explore',
        'st_watch': '/stocktwits/watchlist',
        'iex_quotes_raw': '/prices/eod/all',
        'new_symbols': '/symbols/new',
        'all_symbols': '/symbols/all'
    })

    # Data to conacatenate
    concat = ['st_trend', 'cboe_mmo_top']

    def __init__(self, which):
        df = self.get_data(self, which)
        self.df = df

    @classmethod
    def get_data(cls, self, which):
        """Get data from server."""
        url = f"{self.base_url}{self.url_dict[which]}"
        get = requests.get(url)
        try:
            df = json.load(BytesIO(get.content))
        except JSONDecodeError:
            df = get.json()

        # If data type needs to be looped/concatenated
        if which in self.concat:
            df = self.concat_data(self, df)
            # Clean stocktwits trending data
            if which == 'st_trend':
                df = self._clean_st_trend(self, df)
        else:
            # Convert to dataframe
            df = pd.DataFrame(df)

        return df

    @classmethod
    def concat_data(cls, self, dict):
        """Loop through and concatenate dataframes."""
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










# %% codecell
####################################
