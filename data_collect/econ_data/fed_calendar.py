"""Federal reserve calendar of events."""
# %% codecell
from pathlib import Path
import requests
import json

import pandas as pd
import ast

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_parquet


# %% codecell


class FedResCal():
    """Federal reserve calendar events."""

    def __init__(self):
        fpath = self._get_fpath(self)
        self._get_params(self)
        self.get = self._call_api(self)
        df = self._make_df_transform_data(self, self.get)
        self.df = self._convert_time_data(self, df)
        write_to_parquet(self.df, fpath, combine=True, drop_duplicates=True)

    @classmethod
    def _get_fpath(cls, self):
        """Get fpath for read/writing."""
        bpath = Path(baseDir().path, 'economic_data', 'FED')
        fpath = bpath.joinpath('calendar.parquet')
        return fpath

    @classmethod
    def _get_params(cls, self):
        """Get parameters for API call."""
        url = 'https://www.federalreserve.gov/data/statcalendar.json'

        headers = ({
            'Host': 'www.federalreserve.gov',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:97.0) Gecko/20100101 Firefox/97.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'https://www.federalreserve.gov/data/releaseschedule.htm',
            'Sec-Fetch-Site': 'same-origin',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        })

        self.url = url
        self.headers = headers

    @classmethod
    def _call_api(cls, self):
        """Call API and return get request."""
        get = requests.get(self.url, headers=self.headers)

        if get.status_code != 200:
            msg = f"fed_res_calendar: {get.status_code} {str(get.text)}"
            print(msg)

        return get

    @classmethod
    def _make_df_transform_data(cls, self, get):
        """Make dataframe and start data transformation."""
        data = json.loads(get.content.decode('utf-8-sig'))['events']
        df = pd.DataFrame(data)
        return df

    @classmethod
    def _convert_time_data(cls, self, df):
        """Convert time data and return dataframe."""
        df.dropna(subset='time', inplace=True)
        df['time'] = df['time'].str.replace('.', '', regex=False)
        df['time'] = df['time'].str.replace('a', 'A', regex=False)
        df['time'] = df['time'].str.replace('p', 'P', regex=False)
        df['time'] = df['time'].str.replace('m', 'M', regex=False)

        zsplits = df['time'].str.split(':')
        z0 = zsplits.str[0].str.zfill(2)
        z1 = zsplits.str[1]

        df['time_t'] = (z0 + ':' + z1).str.strip()
        df['time_t'] = pd.to_datetime(df['time_t'], format='%I:%M %p')

        df['hour'] = df['time_t'].dt.hour
        df['minute'] = df['time_t'].dt.minute

        df['days_list'] = df['days'].apply(ast.literal_eval)
        df_exp = df.explode(column='days_list').reset_index(drop=True)

        cols_to_drop = ['time_t', 'type', 'days']
        df_exp.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        df_exp.rename(columns={'days_list': 'days'}, inplace=True)

        df_exp['days'] = df_exp['days'].astype(str).str.zfill(2)
        df_exp['date'] = df_exp['month'] + '-' + df_exp['days']
        df_exp['date'] = pd.to_datetime(df_exp['date'], format='%Y-%m-%d')

        df_exp.drop(columns=['month', 'description', 'days'], inplace=True)

        return df_exp


# %% codecell
