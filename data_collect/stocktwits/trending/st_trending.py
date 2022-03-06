"""Stocktwits trending class."""
# %% codecell

import requests
import pandas as pd

try:
    from scripts.dev.data_collect.stocktwits.helpers.paths import StockTwitsPaths
    from scripts.dev.multiuse.api_helpers import RecordAPICalls
    from scripts.dev.multiuse.help_class import write_to_parquet
except ModuleNotFoundError:
    from data_collect.stocktwits.helpers.paths import StockTwitsPaths
    from multiuse.api_helpers import RecordAPICalls
    from multiuse.help_class import write_to_parquet


# %% codecell


class StTrending(StockTwitsPaths):
    """Stock twits trending data."""

    burl = "https://api.stocktwits.com/api/2/"

    def __init__(self, **kwargs):
        StockTwitsPaths.__init__(self)
        self.get = self._request_data(self, **kwargs)
        self.df = self._create_df(self, self.get, **kwargs)
        self._write_to_file(self, self.df, **kwargs)

    @classmethod
    def _request_data(cls, self):
        """Request data from stocktwits api."""
        # Trending stocks
        trend_url = f"{self.burl}trending/symbols/equities.json"
        payload = {'limit': 29}

        get = requests.get(trend_url, params=payload)
        RecordAPICalls(get, name='stocktwits_trend')

        return get

    @classmethod
    def _create_df(cls, self, get, **kwargs):
        """Create dataframe from response object."""
        df = pd.json_normalize(get.json()['symbols'])
        df['timestamp'] = pd.Timestamp.now('US/Eastern')
        # Drop columns that aren't useful
        (df.drop(columns=['has_pricing', 'is_following'],
                 inplace=True, errors='ignore'))
        return df

    @classmethod
    def _write_to_file(cls, self, df, **kwargs):
        """Write to file."""
        write_to_parquet(df, self.f_trend)
