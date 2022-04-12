"""FED FOMC Calendar."""
# %% codecell
from pathlib import Path
import calendar
from datetime import timedelta

import requests
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup

try:
    from scripts.dev.multiuse.help_class import (getDate, help_print_arg,
                                                 write_to_parquet, baseDir)
except ModuleNotFoundError:
    from multiuse.help_class import (getDate, help_print_arg,
                                     write_to_parquet, baseDir)

# %% codecell


class FedFomc():
    """Federal reserve FOMC meetings past + future."""

    # 2 pm est normal, then press conference after

    def __init__(self, **kwargs):
        self._ffomc_class_vars(self, **kwargs)
        self.df_fomc1 = self._ffomc_get_data(self)
        self.df_fomc2 = self._ffomc_parse_columns(self, self.df_fomc1)
        s_contains = self._ffomc_parse_released_data(self)
        self.df_fomc3 = (self._ffomc_create_released_col(self,
                         s_contains, self.df_fomc2))
        self.df_fomc4 = (self._ffomc_calc_days_until_release(self,
                         self.df_fomc3))
        self._write_to_parquet(self)

    @classmethod
    def _ffomc_class_vars(cls, self, **kwargs):
        """Get federal reserve FOMC class variables."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')

        bpath = Path(baseDir().path, 'economic_data', 'FED')
        self.fpath = bpath.joinpath('fomc.parquet')

        if getattr(self, 'fdict', False):
            self.fdict['fomc_calendar'] = self.fpath
        else:
            self.fidct = {'fomc_calendar': self.fpath}

    @classmethod
    def _ffomc_get_data(cls, self):
        """Get data from fed, parse into basic dataframe."""
        burl = 'https://www.federalreserve.gov'
        furl = f'{burl}/monetarypolicy/fomccalendars.htm'
        # Request data
        get = requests.get(furl)
        # Initialize beautiful soup object
        soup = BeautifulSoup(get.content.decode('utf-8'), 'html.parser')
        # Iterate through relevant data rows
        row_list = []
        fomc_rows = soup.find_all("div", class_="fomc-meeting")
        for row in fomc_rows:
            row_list.append(list(row.stripped_strings))
        # Concat into dataframe
        df_rows = pd.DataFrame(row_list)
        return df_rows

    @classmethod
    def _ffomc_parse_columns(cls, self, df_r1):
        """Parse columns."""
        df_r2 = df_r1.loc[:, :1].copy()
        df_r2 = (df_r2.join(df_r2[1].str
                 .split(' ', expand=True)
                 .rename(columns={0: 'days', 1: 'reason'})
                 .drop(columns=2))
                 .rename(columns={0: 'ogMonth'})
                 .copy())

        # * Meeting associated with a Summary of Economic
        # Projections and a press conference by the Chair.
        df_r2['pressConf'] = df_r2['days'].str.contains('\*')
        df_r2['lastDay'] = (df_r2['days'].str.replace('\*', '', regex=True)
                                         .str.extractall('([0-9]{1,2})')
                                         .groupby(level=[0])
                                         .tail(1)[0]
                                         .reset_index(level=1, drop=True)
                                         .str.zfill(2))
        df_r2['reason'] = (df_r2['reason'].str.strip()
                           .replace(r'[()]', '', regex=True))
        df_r2['lastMonth'] = (df_r2['ogMonth'].str
                              .split('/')
                              .apply(lambda x: x[-1]))
        df_r2['nMonths'] = (df_r2['ogMonth'].str
                            .split('/').str
                            .len())
        return df_r2

    @classmethod
    def _ffomc_parse_released_data(cls, self):
        """Create released column for other dataframe columns."""
        df_r11 = self.df_fomc1.loc[:, 2:].dropna(how='all')
        row_list_test = []
        for col in df_r11.columns:
            smatch = (df_r11[col].str
                      .contains('Released')
                      .replace(False, np.NaN)
                      .dropna())
            if not smatch.empty:
                row_list_test.append(df_r11.loc[smatch.index, col])

        s_contains = pd.concat(row_list_test).sort_index()
        return s_contains

    @classmethod
    def _ffomc_create_released_col(cls, self, s_contains, df_r2):
        """Create released column."""
        df_r2['released'] = np.NaN
        df_r2.loc[s_contains.index, 'released'] = s_contains

        df_r2['released'] = (df_r2['released'].str
                             .replace('\(Released|\)', '', regex=True)
                             .str.strip())

        df_r2['released'] = (pd.to_datetime(df_r2['released'],
                             format='%B %d, %Y'))

        df_r2['og_year'] = ((df_r2['released'] - pd.Timedelta(days=21))
                            .dt.year
                            .ffill()
                            .astype('int')
                            .astype('str'))

        mdict = ({abr: name for abr, name in
                 zip(calendar.month_abbr, calendar.month_name)})
        df_r2['lastMonth'].update(df_r2['lastMonth'].map(mdict).dropna())

        df_r2['meetingDate'] = (pd.to_datetime((df_r2['lastMonth']
                                + df_r2['lastDay'] + df_r2['og_year']),
                                format='%B%d%Y'))

        cols_to_drop = (['ogMonth', 1, 'days', 'og_year',
                         'lastDay', 'lastMonth'])
        df_r3 = (df_r2.drop(columns=cols_to_drop)
                      .copy())
        return df_r3

    @classmethod
    def _ffomc_calc_days_until_release(cls, self, df_r3):
        """Calulate the days until the next fomc meeting note release."""
        dt = getDate.query('iex_close')
        df_r3['futMeeting'] = df_r3['meetingDate'].dt.date > dt
        df_r3['d_until_release'] = pd.to_datetime(dt)
        df_r3['estRelease'] = df_r3['meetingDate'] + timedelta(days=21)
        df_r3['d_until_release'] = (getDate.get_bus_day_diff(
                                    df_r3, 'd_until_release',
                                    'estRelease'))

        return df_r3

    @classmethod
    def _write_to_parquet(cls, self):
        """Write to parquet."""
        write_to_parquet(self.df_fomc4, self.fpath)
        if self.verbose:
            help_print_arg(f"FedFomc: written to fpath: {str(self.fpath)}")
