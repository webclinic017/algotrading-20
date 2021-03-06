"""Earnings webscraped."""
# %% codecell
from pathlib import Path
import os
from time import sleep
from random import randint

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet, help_print_arg
    from scripts.dev.multiuse.api_helpers import RecordAPICalls
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet, help_print_arg
    from multiuse.api_helpers import RecordAPICalls

# %% codecell


class ScrapedEE():
    """Scraped Earnings Estimates."""

    """
    var: self.fpath = economic_data/analyst_earnings
    var: self.dt = date to use (iex_close)
    var: self.headers, self.url, self.params : obvious
    var: self.get : get request whether failure/success
    var: self.df : dataframe if get request succeeds

    """

    def __init__(self, dt=None):
        self._define_fpath(self, dt)
        self._build_headers_url_params(self)
        self.get = self._request_data_and_store(self)
        self.df = self._clean_return_dataframe(self, self.get)

    @classmethod
    def _define_fpath(cls, self, dt):
        """Define and construct local fpath."""
        if not dt:
            dt = getDate.query('iex_close')

        bpath = Path(baseDir().path, 'economic_data', 'analyst_earnings')
        fpath = bpath.joinpath(f"_{str(dt.year)}", f"_{dt}.parquet")

        self.dt = dt
        self.fpath = fpath

    @classmethod
    def _build_headers_url_params(cls, self):
        """Build header dict."""
        load_dotenv()

        headers = ({
            'Host': os.environ.get('ee_host'),
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:96.0) Gecko/20100101 Firefox/96.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': os.environ.get('ee_ref'),
            'Origin': os.environ.get('ee_ref'),
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Sec-GPC': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        })

        self.headers = headers
        self.url = os.environ.get('ee_url')
        self.payload = {'date': str(self.dt)}

    @classmethod
    def _request_data_and_store(cls, self):
        """Request data and convert to dataframe. Write locally."""

        get = (requests.get(self.url,
                            headers=self.headers,
                            params=self.payload))
        # Record the call in local log
        RecordAPICalls(get, name='analyst_prices')
        return get

    @classmethod
    def _clean_return_dataframe(cls, self, get):
        """Process response object, clean and return dataframe."""
        if get.status_code < 400:
            df = pd.DataFrame(get.json()['data']['rows'])
            df['date'] = self.dt
            df = CleanScrapedEE(df, self.fpath).df
            return df
        else:
            msg = f"Scraped EE failed with msg {str(get.content)}"
            help_print_arg(msg)
            return None


# %% codecell


class CleanScrapedEE():
    """Clean scraped analyst ratings."""

    def __init__(self, df, fpath):
        cleaned_df = self._clean_convert_nans(self, df)
        convert_df = self._convert_cols(self, cleaned_df)
        self.df = self._write_dataframes(self, convert_df, fpath)

    @classmethod
    def _clean_convert_nans(cls, self, df):
        """Clean and convert nans."""
        cols = df.columns
        df.dropna(subset=['symbol'], inplace=True)

        cols_from_neg_to_min = ['eps', 'surprise', 'epsForecast', 'lastYearEPS']
        for col in cols_from_neg_to_min:
            if col in cols:
                df[col] = np.where(
                    df[col].str.contains('[()]'),
                    df[col].str.replace('[()]', '-', regex=True, n=1),
                    df[col]
                )

                df[col] = df[col].replace('[\), $]', '', regex=True)

        cols_to_remove_na = ['eps', 'surprise', 'epsForecast', 'noOfEsts']
        for col in cols_to_remove_na:
            if col in cols:
                df[col] = np.where(
                    df[col].str.contains('N/A'),
                    df[col].replace('N/A', np.NaN),
                    df[col]
                )

        cols_to_dt = ['lastYearRptDt']
        for col in cols_to_dt:
            if col in cols:
                df[col] = (pd.to_datetime(df[col],
                           format='%m/%d/%Y', errors='coerce'))

        df['marketCap'] = df['marketCap'].replace('[$,\,]', '', regex=True)

        return df

    @classmethod
    def _convert_cols(cls, self, df):
        """Convert columns to the right data type."""
        cols_to_float = ['noOfEsts', 'eps', 'surprise', 'epsForecast']
        cols = df.columns

        for col in cols_to_float:
            if col in cols:
                try:
                    df[col] = df[col].astype(np.float64)
                except ValueError:
                    print(col)

        df['symbol'] = df['symbol'].astype('category')

        return df

    @classmethod
    def _write_dataframes(cls, self, df, fpath):
        """Write dataframe or combine, if need be."""
        if fpath.exists():
            try:
                df_old = pd.read_parquet(fpath)

                cols_to_lose = (['time', 'name', 'marketCap',
                                 'fiscalQuarterEnding',
                                 'epsForecast', 'noOfEsts'])

                df2 = df_old.drop(columns=cols_to_lose)
                if 'symbol' in df2.columns:
                    df2.set_index('symbol', inplace=True)
                df1 = df.set_index('symbol')

                df_all = df1.join(df2, on='symbol')
                write_to_parquet(df_all, fpath)

            except Exception as e:
                write_to_parquet(df, fpath)
        else:
            write_to_parquet(df, fpath)

        return df
