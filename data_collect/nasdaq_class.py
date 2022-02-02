"""
Nasdaq data classes.

Q - NASDAQ Global Select Market (NGS)
R - NASDAQ Capital Market
Short data comes out every day past 4:30 pm
"""

# %% codecell
############################################
from pathlib import Path
from io import BytesIO
import os
import datetime

import requests
import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.create_file_struct import makedirs_with_permissions
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate, help_print_error, help_print_arg, write_to_parquet
    from multiuse.create_file_struct import makedirs_with_permissions

# %% codecell


def get_nasdaq_symbol_changes():
    """Get symbol change history from nasdaq."""
    sym_change_url = 'https://api.nasdaq.com/api/quote/list-type-extended/symbolchangehistory'

    nasdaq_headers = ({
        'Host': 'api.nasdaq.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:94.0) Gecko/20100101 Firefox/94.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Origin': 'https://www.nasdaq.com',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nasdaq.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Sec-GPC': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    })

    get = requests.get(sym_change_url, headers=nasdaq_headers)

    df_sym_change = None
    if get.status_code == 200:
        df_sym_change = (pd.DataFrame(get.json()['data']
                         ['symbolChangeHistoryTable']['rows']))
    else:
        msg1 = 'get_nasdaq_symbol_changes failed with url'
        msg2 = f"and status code {str(get.status_code)}"
        help_print_arg(f"{msg1} {sym_change_url} {msg2}")

    dt = getDate.query('iex_close')
    path = (Path(baseDir().path, 'ref_data/symbol_ref/symbol_changes',
                 f'_{dt}.parquet'))

    if isinstance(df_sym_change, pd.DataFrame):
        write_to_parquet(df_sym_change, path)
    else:
        raise Exception


class nasdaqShort():
    """Get daily circut breaker short data."""

    fbase = f"{baseDir().path}/short/daily_breaker"
    sh_base = "http://nasdaqtrader.com/dynamic/symdir/shorthalts/shorthalts"

    def __init__(self, rpt_date):
        self.dt = rpt_date
        self.rpt_date = rpt_date.strftime('%Y%m%d')
        self.df = self.get_data(self)

    @classmethod
    def get_data(cls, self):
        """Check for local parquet file."""
        self.fpath = f"{self.fbase}/nasdaq_{self.dt}.parquet"
        local_df = False

        if os.path.isfile(self.fpath):
            local_df = pd.read_parquet(self.fpath)
        else:
            local_df = self._get_request(self)
            self._write_to_parq(self, local_df)

        return local_df

    @classmethod
    def _get_request(cls, self):
        """Get data from nasdaq circuit breaker."""
        sh_url = f"{self.sh_base}{self.rpt_date}.txt"

        sh_get = requests.get(sh_url)
        # Convert to pandas dataframe
        sh_df = self._convert_to_pandas(self, sh_get)

        return sh_df

    @classmethod
    def _convert_to_pandas(cls, self, sh_get):
        """Convert data to pandas dataframe."""
        sh_df = pd.read_csv(
                    BytesIO(sh_get.content),
                    escapechar='\n',
                    delimiter=',',
                    skipinitialspace=False
                    )
        # Convert object columns to category columns
        sh_df = dataTypes(sh_df, parquet=True).df

        return sh_df

    @classmethod
    def _write_to_parq(cls, self, local_df):
        """Write to local parquet file."""
        write_to_parquet(local_df, self.fpath)

# %% codecell


class NasdaqHalt():
    """Get a list of halted symbols every minute."""

    """
    Class variables:
        self.df : dataframe from either today, most recent, or all
        self.base_path : base directory to write files
        self.path : most recent path
    """

    """
    Cols definitions: https://www.nasdaqtrader.com/Trader.aspx?id=TradeHaltCodes
    Halt Codes:
        T1 : News pending
        T2 : News released
        T5 : Trading paused - 10% move in 5 minute period
        T6 : Extraordinary market activity
        T8 : ETF halt
        T12 : Halt for additional info

        H4: Non-compliance with listing requirements
        H9: Not current in required filings
        H10: Sec trading suspension
        H11: Regulatory concern

        O1: Operations halt
        IPO1: IPO issue not yet trading
        M1: Corporate action
        M2: Quotation not available
        LUDP: Volatility trading pause
        LUDS: Volatility trading pause - straddle condition

        MWC1 - Market wide circuit breaker - level 1
        M2C2 - Market wide circuit breaker - level 2
        MWC3 - Market wide circuit breaker - level 3
        MWC0 - Market wide circuit breaker halt from previous day

        T3 - News and resumption times
        T7 - Quotations resumed, but trading still paused
        R4 - Qualification issues reviewed, trading to resume
        R9 - Filing requirement satisfied, trading to resume
        C3 - Issuer news not forthcoming, trading to resume

        R1 - New issue available
        R2 - Issue available
        IPOQ - IPO security released for quotation
        IPOE - IPO security - positioning window extended
        M - Volatility trading pause
    """

    def __init__(self, read=False, all=False):
        self.get_path(self)

        if read is False:
            self._get_data(self)
            self._process_data(self)
        elif read is True:
            self._read_data(self, all=all)

    @classmethod
    def get_path(cls, self):
        """Get fpath."""
        dt = getDate.query('sec_rss')
        base_path = Path(baseDir().path, 'short/halts')

        if not base_path.exists():
            makedirs_with_permissions(base_path)

        self.base_path = base_path
        self.path = Path(base_path, f"_{dt}.parquet")

    @classmethod
    def _get_data(cls, self):
        """Get nasdaq halt data."""
        url = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
        get = requests.get(url)
        # If for some reason the request failed
        if get.status_code >= 400:
            e = 'Requests Error'
            parent = str(type(self).__name__)
            help_print_error(e, parent=parent, resp=get)
        self.get = get

    @classmethod
    def _process_data(cls, self):
        """Convert from xml, clean, and process."""
        df = pd.read_xml(self.get.content, xpath='.//item')

        col_list = []
        for col in df.columns:
            if '}' in str(col):
                # print(col.split('}')[1])
                col_list.append(col.split('}')[1])
            else:
                col_list.append(col)

        df.columns = col_list
        df.drop(columns=['description', 'PauseThresholdPrice'], inplace=True)

        self.df = df

        if self.path.exists():
            df_prev = pd.read_parquet(self.path)
            subset = ['HaltTime', 'IssueSymbol']
            df_all = (pd.concat([df_prev, df])
                        .reset_index(drop=True)
                        .drop_duplicates(subset=subset))
            write_to_parquet(df_all, self.path)
        else:
            write_to_parquet(df, self.path)

    @classmethod
    def _read_data(cls, self, all=False):
        """Read local dataframe for today, or most recent, or all."""
        if all:
            df_list = []
            for fpath in list(self.base_path.glob('*.parquet')):
                df_list.append(pd.read_parquet(df_list))
            df_all = pd.concat(df_list).reset_index(drop=True)
            self.df = df_all
        else:
            self.df = pd.read_parquet(self.path)

# %% codecell
