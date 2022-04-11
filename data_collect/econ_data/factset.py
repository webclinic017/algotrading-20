"""Factset sources on president, white house, covid."""
# %% codecell
from pathlib import Path

import requests
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg, write_to_parquet

# %% codecell


class FactSetSources():
    """Handling data sources from factset."""

    def __init__(self, method, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.fpath = self._fss_get_fpaths(self, method, **kwargs)
        self.url = self._fss_get_url(self, method, **kwargs)
        if self.fpath and self.url:
            self.get = self._fss_get_data(self, method, **kwargs)
            self.df = self._fss_process_data(self, method, **kwargs)
            self._fss_write_to_parquet(self, method, **kwargs)

    @classmethod
    def _fss_get_fpaths(cls, self, method, **kwargs):
        """Get fpath dict for factset dataframes."""
        bdir = Path(baseDir().path, 'economic_data', 'factset')

        fdict = ({
            'covid': bdir.joinpath('_covid_cases.parquet'),
            'biden': bdir.joinpath('_pres_schedule.parquet'),
            'white_house_news': bdir.joinpath('_white_house_news.parquet')
        })

        fpath = fdict.get(method, False)
        if self.verbose:
            help_print_arg(f"FactSetSources: {method} {str(fpath)}")
        return fpath

    @classmethod
    def _fss_get_url(cls, self, method, **kwargs):
        """Get url from options."""
        udict = ({
            'covid': 'https://media-cdn.factba.se/rss/json/coronavirus.json',
            'biden': 'https://media-cdn.factba.se/rss/json/calendar-full.json',
            'white_house_news': 'https://factba.se/rss/latest.json'
        })

        url = udict.get(method, None)
        return url

    @classmethod
    def _fss_get_data(cls, self, method, **kwargs):
        """Get data from url and return the response object."""
        get = requests.get(self.url)
        return get

    @classmethod
    def _fss_process_data(cls, self, method, **kwargs):
        """Process data and return dataframe."""
        df = None  # Assign empty variable
        if method == 'covid':
            df = pd.json_normalize(self.get.json()['countries'].values())
            df['data_updated'] = pd.to_datetime(df['data_updated'])
            df['last_bot_check'] = pd.to_datetime(df['last_bot_check'])
        elif method == 'biden':
            df = pd.json_normalize(self.get.json())
            pres_cols = df.columns
            covid_cols = pres_cols[pres_cols.str.contains('coronavirus')]

            df_us_covid = df[covid_cols].copy()
            df_pres = df[pres_cols.difference(covid_cols)].copy()
            df_pres['date'] = pd.to_datetime(df_pres['date'])
            return df_pres, df_us_covid
        elif method == 'white_house_news':
            df = pd.DataFrame(self.get.json())
            df['publish'] = pd.to_datetime(df['publish'])

        return df

    @classmethod
    def _fss_write_to_parquet(cls, self, method, **kwargs):
        """Write to parquet, insert any method dependent kwargs if needed."""
        if method == 'biden':
            write_to_parquet(self.df[0], self.fpath, combine=True)
            fpath_cov = self.fpath.parent.joinpath('_us_covid_trends.parquet')
            write_to_parquet(self.df[1], fpath_cov, combine=True)
        elif method == 'white_house_news':
            kwargs = {'cols_to_drop': ['feed_id']}
            write_to_parquet(self.df, self.fpath, combine=True, **kwargs)
        else:
            write_to_parquet(self.df, self.fpath, combine=True)
