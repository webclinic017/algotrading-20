"""Data pipeline classes for Bz."""
# %% codecell
from pathlib import Path
import string
import sys
from time import sleep
from datetime import date

import pandas as pd
import requests
from seleniumwire import webdriver
from selenium.webdriver import FirefoxOptions
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import WebDriverException


try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg, df_create_bins
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet, help_print_arg, df_create_bins
    from api import serverAPI

# %% codecell


class BzRecs():
    """Get analyst recommendations."""

    """
    self.df : og df : str cols not converted
    self.con_df : converted column df
    self.benz_df : final dataframe before write_to_parquet
    self.path : path to write
    """

    def __init__(self):
        self._get_clean_data(self)
        self._object_cols_to_floats(self, self.df)
        self._write_to_parquet(self, self.con_df)

    @classmethod
    def _get_clean_data(cls, self):
        """Get and clean analyst recommendations."""
        rec_url = 'https://www.benzinga.com/analyst-ratings'
        df = pd.read_html(rec_url)[0]
        if df.empty:
            help_print_arg("BzRecs dataframe empty. Check url/request for failure")
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
        try:
            df.drop(columns=['Get Alert'], inplace=True)
        except KeyError:
            pass

        # Convert object columns into floats as needed
        df['Current price'] = df['Current price'].str.replace('$', '', regex=False)
        df['Upside/Downside'] = df['Upside/Downside'].str.replace('%', '', regex=False)
        df['prev_price'] = df['Price Target Change'].str.split('$').str[0]
        df['new_price'] = df['Price Target Change'].str.split('$').str[-2]
        self.df = df

    @classmethod
    def _object_cols_to_floats(cls, self, df):
        """Convert object columns to floats."""
        cols = ['Current price', 'Upside/Downside', 'prev_price', 'new_price']
        unwanted = string.ascii_letters + string.punctuation + string.whitespace + '\u2192'
        unwanted = unwanted.replace('.', '').replace('-', '')
        stringCols = df.dtypes[df.dtypes == 'object']

        for col in cols:
            if col in stringCols:
                df[col] = pd.to_numeric(df[col].str.strip(unwanted), errors='coerce')

        (df.rename(columns={'Date': 'date', 'Ticker': 'symbol',
                            'Upside/Downside': 'perc_change',
                            'Current price': 'mark'},
                   inplace=True))

        self.con_df = df.copy()

    @classmethod
    def _write_to_parquet(cls, self, con_df):
        """Write to parquet/concat if needed."""
        benz_all_df, benz_df = None, None

        bpath = Path(baseDir().path, 'company_stats', 'analyst_recs')
        path = Path(bpath, f"_{getDate.query('iex_close')}.parquet")
        path_all = Path(bpath, 'analyst_recs.parquet')

        if path.exists():
            df_old = pd.read_parquet(path)
            benz_df = (pd.concat([df_old, con_df])
                         .drop_duplicates(subset= ['date', 'symbol', 'Analyst Firm'])
                         .reset_index(drop=True))
        else:
            benz_df = con_df.copy()

        if path_all.exists():
            df_old = pd.read_parquet(path_all)
            benz_all_df = (pd.concat([df_old, con_df])
                             .drop_duplicates(subset=['date', 'symbol', 'Analyst Firm'])
                             .reset_index(drop=True))
        else:
            benz_all_df = con_df.copy()

        self.path = str(path)
        self.benz_df = benz_df.copy()
        write_to_parquet(benz_df, path)
        write_to_parquet(benz_all_df, path_all)

# %% codecell


def get_hist_bz_ratings(cutoff=None, start_dt=None, this_year=True):
    """Initiate data collection of past analyst ratings."""
    # Pull analyst ratings (default is this year)
    recs = serverAPI('analyst_recs_scraped').df
    rec_dates = recs['date'].dt.date.unique()

    start_dt = date(2021, 1, 1)

    dt = getDate.query('iex_eod')
    bus_days = (getDate.get_bus_days(this_year=this_year,
                                     cutoff=dt, start_dt=start_dt))

    dates_needed = bus_days[~bus_days['date'].isin(rec_dates)].copy()
    dt_w_bins = df_create_bins(dates_needed, bin_size=3)

    for bin in dt_w_bins['bins'].unique():
        df_mod = dt_w_bins[dt_w_bins['bins'] == bin].copy()
        dt_min = df_mod['date'].dt.date.min()
        dt_max = df_mod['date'].dt.date.max()

        WebScrapeBzRates(dt_min=dt_min, dt_max=dt_max)


class WebScrapeBzRates:
    """Webscraping bz analyst ratings."""

    """
    self.fdrive : firefox driver
    self.df_resp : dataframe of urls in request once landed on page
    self.get : get request for XHR bz
    self.df_recs : dataframe of all analyst recommendations
    """

    def __init__(self, testing=False, dt_min=False, dt_max=False):
        self._initiate_browser(self)
        self._iterate_through_result(self, self.fdrive)
        self._parse_df_submit_get_request(self, self.df_resp, dt_min, dt_max)
        self._parse_get_request(self, self.get)

    @classmethod
    def _initiate_browser(cls, self):
        """Start headless browser and navigate to page."""
        fdrive = None  # Create empty variable
        try:
            opts = FirefoxOptions()
            opts.add_argument("--headless")
            path = '/usr/local/bin/geckodriver'
            fdrive = webdriver.Firefox(executable_path=path, options=opts)
        except WebDriverException:
            fdrive = (webdriver.Firefox(
                      executable_path=GeckoDriverManager().install()))

        burl = 'https://www.benzinga.com/analyst-ratings'
        fdrive.get(burl)
        fdrive.implicitly_wait(5)
        sleep(5)

        self.fdrive = fdrive

    @classmethod
    def _iterate_through_result(cls, self, fdrive):
        """Iterate through firefox driver result."""
        resp_list = []
        for request in fdrive.requests:
            if request.response:
                rdict = {}
                rdict['url'] = request.url
                rdict['params'] = request.params
                rdict['querystring'] = request.querystring
                rdict['headers'] = request.headers
                resp_list.append(rdict)

        df_resp = pd.DataFrame(resp_list)
        self.df_resp = df_resp
        # Close browser
        self.fdrive.close()

    @classmethod
    def _parse_df_submit_get_request(cls, self, df_resp, dt_min, dt_max):
        """Get required params from dataframe, make new request."""
        r0 = None
        url = 'https://api.benzinga.com/api/v2.1/calendar/ratings'
        recs_row = (df_resp[df_resp['url'].str.contains(url)]
                    .reset_index(drop=True))

        if recs_row.empty:
            msg = "WebScrapeBzRates: Can't find url in df_resp"
            help_print_arg(msg)
            sys.exit()

        try:
            r0 = recs_row.index[0]
        except KeyError:
            help_print_arg(str(recs_row))
            r0 = recs_row.index.tolist()[0]

        headers = dict(recs_row['headers'].iloc[r0])
        params = recs_row['params'].iloc[r0]

        # Optional parameters for gathering historical data
        if dt_min:
            params['parameters[date_from]'] = dt_min
        if dt_max:
            params['parameters[date_to]'] = dt_max

        # Get data
        get = requests.get(url, headers=headers, params=params)
        if get.status_code != 200:
            msg1 = f"Bz scrape ratings get request{get.status_code}"
            msg2 = f"with params{str(params)}"
            msg = f"{msg1}{msg2}"
            help_print_arg(msg)

        self.headers = headers
        self.params = params
        self.get = get

    @classmethod
    def _parse_get_request(cls, self, get):
        """Parse submitted get request."""
        bpath = Path(baseDir().path, 'company_stats', 'analyst_recs')

        df_recs = pd.DataFrame(get.json()['ratings'])
        df_recs['date'] = pd.to_datetime(df_recs['date'])
        yr = df_recs['date'].max().year
        fpath = bpath.joinpath(f"_{str(yr)}.parquet")

        if fpath.exists():
            df_old = pd.read_parquet(fpath)
            df_recs = (pd.concat([df_old, df_recs])
                         .drop_duplicates(subset=['id'])
                         .reset_index(drop=True))

        write_to_parquet(df_recs, fpath)

        self.df_recs = df_recs
