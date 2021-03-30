"""
Can we do free batch processing from the OCC?
Flex Number Prefix:
1 = American-style equity. For index products, this also means it is a.m. settled.
2 = European-style equity. For index products, this also means it is a.m. settled.
3 = American-style index, p.m. settled.
4 = European-style index, p.m. settled.
"""

# %% codecell
############################################################
import os
import os.path
import json
from json import JSONDecodeError
from io import StringIO, BytesIO
import glob
import importlib
import sys
import xml.etree.ElementTree as ET

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

import pytz
import datetime
from datetime import date, timedelta, time

from charset_normalizer import CharsetNormalizerMatch
from charset_normalizer import detect
from charset_normalizer import CharsetNormalizerMatches as CnM

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate
    from scripts.dev.data_collect.options import DerivativeExpirations, DerivativesHelper
    from scripts.dev.data_collect.iex_class import readData, urlData, marketHolidays
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate

    from data_collect.options import DerivativeExpirations, DerivativesHelper
    importlib.reload(sys.modules['data_collect.options'])
    from data_collect.options import DerivativeExpirations, DerivativesHelper

    from data_collect.iex_class import marketHolidays


# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', None)

# %% codecell
############################################################



"""
# %% codecell
############################################################
test_nf = occ_flex_pr.occ_df.copy(deep=True)
test_nf.head(10)
# report_date = DerivativesHelper.which_fname_date()
report_date = datetime.date(2021, 2, 12)
con_df = TradeVolume(report_date, 'con_volume')
con_df.vol_df.head(10)
# %% codecell
############################################################
"""
class occFlex():
    """Get all equity options data for report_date."""

    # report_type: OI = Open Interest, PR = Price
    # option_type: E = Equity. I = Index
    occ_burl = "https://marketdata.theocc.com"
    dump_dir = f"{baseDir().path}/derivatives/occ_dump"

    def __init__(self, report_type, option_type, report_date):
        """Initialize class."""
        # self.report_date = self.right_report_date(self)
        self.report_date = report_date
        self.report_type = report_type
        self.fname = self.right_fname(self)

        self.df = self.read_local_data(self)
        # self.df = False
        if not isinstance(self.df, pd.DataFrame):
            self.flex_bytes = self.get_data(self, report_type, option_type)
            # self.flex_bytes = flex_bytes
            self.occ_nf = self.read_csv(self)  # No format
            # self.occ_df = self.occ_nf
            self.occ_rc = self.fix_col_names(self)  # Renamed cols
            self.df = self.clean_data(self)
            self.write_to_json(self)

    @classmethod
    def right_report_date(cls, self):
        """Check if correct/change if wrong."""
        trading_days = marketHolidays(type='trade')
        last_trading_day = trading_days.days_df['date'][0]
        last = datetime.datetime.strptime(last_trading_day, '%Y-%m-%d').date()
        return last

    @classmethod
    def right_fname(cls, self):
        """Form the right fname for local dir."""
        rpt_lower = self.report_type.lower()

        fname_p1 = f"/{rpt_lower}/equity_{rpt_lower}_{self.report_date}.gz"
        fname = f"{self.dump_dir}{fname_p1}"
        # Return fname to self.fname
        return fname

    @classmethod
    def read_local_data(cls, self):
        """See if data is available in local directory."""
        if os.path.isfile(self.fname):
            local_df = pd.read_json(self.fname)
        else:
            local_df = False

        return local_df

    @classmethod
    def get_data(cls, self, report_type, option_type):
        """Construct and make get request."""
        rpt_date = self.report_date.strftime('%Y%m%d')
        p1_url = f"{self.occ_burl}/flex-reports?reportType={report_type}"
        p2_url = f"&optionType={option_type}&reportDate={rpt_date}"
        # Make get request with passed url
        flex_bytes = requests.get(f"{p1_url}{p2_url}")

        # If a short error message assume wrong date
        if len(flex_bytes.content) < 500:
            self.report_date = self.report_date - timedelta(days=1)
            rpt_date = self.report_date.strftime('%Y%m%d')
            p2_url = f"&optionType={option_type}&reportDate={rpt_date}"
            # Make get request with passed url
            flex_bytes = requests.get(f"{p1_url}{p2_url}")

        self.byte_length = len(flex_bytes.content)
        self.rpt_to_print = CnM.from_bytes(flex_bytes.content).best().first()

        return flex_bytes

    @classmethod
    def read_csv(cls, self):
        """Pandas read csv with params."""
        occ_nf = pd.read_csv(
                    BytesIO(self.flex_bytes.content),
                    escapechar='\n',
                    delimiter=',',
                    delim_whitespace=True,
                    skipinitialspace=True
                    )
        # If dataframe has a multiIndex
        if self.report_type == 'PR':
            occ_nf.reset_index(inplace=True)

        return occ_nf

    @classmethod
    def fix_col_names(cls, self):
        """Rename columns from row data."""
        df = self.occ_nf.copy(deep=True)

        # Get the first column
        col_first = df.columns[0]

        # Find row with column name data
        col_row = df[df[col_first] == 'SYMBOL'].head(1).index[0]
        # Get column row names
        df_cols = df.iloc[col_row].values
        # Rename the 8th column to what looks like volume
        if self.report_type == 'OI':
            df_cols[6] = 'SRIKE'
            df_cols[7] = 'MARK'
            df_cols[8] = 'OI'
            df.columns = df_cols
            df.rename(columns={'P/C': 'SIDE', 'DA': 'DAY'}, inplace=True)
        elif self.report_type == 'PR':
            # SRIKE - UNDERLYING - MARK
            df_cols[5] = 'STRIKE'
            df_cols[6] = 'UNDERLYING'
            df_cols[7] = 'MARK'
            df.columns = df_cols
            df.rename(columns={'C': 'SIDE', 'DA': 'DAY', 'YEAR': 'YR'}, inplace=True)

        return df

    @classmethod
    def clean_data(cls, self):
        """Clean and add columns."""
        # Exclude all rows that don't contain C and P in the P/C column
        occ_ff = self.occ_rc.copy(deep=True)
        occ_ff = occ_ff[occ_ff['SIDE'].isin(['C', 'P'])]
        occ_ff = occ_ff[~occ_ff['SYMBOL'].isin(['SYMBOL'])]
        # Drop columns that contain all NaNs
        occ_ff.dropna(axis=1, how='all', inplace=True)

        # Take the first character of symbols and make new col
        occ_ff['STYLE'] = occ_ff['SYMBOL'].str[0]  # 1 = American, 2 = European
        occ_ff['SYMBOL'] = occ_ff['SYMBOL'].str[1:]
        # Create column with formatted expiration dates - %Y%m%d
        occ_ff['expirationDate'] = occ_ff['YR'] + occ_ff['MO'] + occ_ff['DAY']

        # Reset index and drop inplace
        occ_ff.reset_index(drop=True, inplace=True)

        occ_ff['expirationDate'] = occ_ff['expirationDate'].astype('category')
        # Need to label data types and too lazy at the moment
        # occ_ff = dataTypes(occ_ff).df
        return occ_ff

    @classmethod
    def write_to_json(cls, self):
        """Write dataframe to local json file."""
        # Write to local json file
        self.df.to_json(self.fname, compression='gzip')


# %% codecell
##############################################################

class tradeVolume():
    """Batch processing from OCC Trade Volume."""
    dump_dir = f"{baseDir().path}/derivatives/occ_dump/vol"
    base_url = "https://marketdata.theocc.com/"
    vol_df = ''
    # query can be 'con_volume' for contract volume
    # or 'trade_volume'
    def __init__(self, report_date, query, fresh):
        self.report_date = report_date
        self.fname = self.create_fname(self, query)
        self.url = self.create_url(self, query)
        if not fresh:
            self.vol_df = self.read_local_data(self)
        print('df.vol_df to get dataframe')
        if not isinstance(self.vol_df, pd.DataFrame):
            self.xml_data = self.get_trade_data(self)
            self.vol_df = self.process_data(self)
            self.convert_col_dtypes(self)
            self.write_to_json(self)
        #"""

    @classmethod
    def create_fname(cls, self, query):
        """Determine local file path name."""
        if query == 'con_volume':
            f_suf = f"/contrades_{self.report_date}.gz"
        elif query == 'trade_volume':
            f_suf = f"/tradevol_{self.report_date}.gz"
        else:
            print('Wrong params. Use either con_volume or trade_volume')
        # Combine base dir with f_suf for filepath name
        fname = f"{self.dump_dir}{f_suf}"
        return fname

    @classmethod
    def create_url(cls, self, query):
        # Format date for get request
        rpt_date = self.report_date.strftime("%Y%m%d")
        # Determine url based on query data
        if query == 'con_volume':
            url_pre = "/cont-volume-download"
        elif query == 'trade_volume':
            url_pre = "/trade-volume-download"
        else:
            print('Wrong params. Use either con_volume or trade_volume')

        # Format date for get request
        rpt_date = self.report_date.strftime("%Y%m%d")
        url_suf = f"?reportDate={rpt_date}&format=xml"

        # Full url to use
        url = f"{self.base_url}{url_pre}{url_suf}"
        return url

    @classmethod
    def read_local_data(cls, self):
        """See if data is available in local directory."""
        # Load base_directory (derivatives data)
        if os.path.isfile(self.fname):
            local_df = pd.read_json(self.fname)
        else:
            local_df = False

        return local_df

    @classmethod
    def get_trade_data(cls, self):
        """Get trade volume from iex."""
        occ_get = requests.get(self.url)
        root = ET.fromstring(occ_get.content.decode('ISO-8859-1'))
        return root

    @classmethod
    def process_data(cls, self):
        """Parse tag list and convert data to dataframe."""
        vol_dict = {}
        # Column names for 9 unique cols
        col_tags = list(set([tg.tag for tg in self.xml_data.findall('./record/*')[:15]]))
        for tg in col_tags:
            vol_dict[tg] = [tg.text for tg in self.xml_data.findall(f"./record/{tg}")]
        # Convert to dataframe
        vol_df = pd.DataFrame(vol_dict)

        return vol_df

    @classmethod
    def convert_col_dtypes(cls, self):
        """Convert column dtypes."""

        # Rename the columns
        self.vol_df.rename(columns={
                'exchangeId': 'exId',
                'exchangeName': 'exName',
                'firmQuantity': 'fQuant',
                'customerQuantity': 'cQuant',
                'marketQuantity': 'mQuant'}, inplace=True)

        vol_cols = self.vol_df.columns
        # Columns to convert to category
        cols_to_cat = (['symbol', 'pkind', 'exName', 'exId',
                        'actdate', 'underlying'])
        self.vol_df[cols_to_cat] = self.vol_df[cols_to_cat].astype('category')

        # Get the columns to convert to integer
        # cols_to_int = [col for col in vol_cols if col not in cols_to_cat]
        # self.vol_df[cols_to_int] = self.vol_df[cols_to_int].astype(np.uint16)
        # self.vol_df = dataTypes(self.vol_df).df


    @classmethod
    def write_to_json(cls, self):
        """Write file to local json."""
        # Write to local json
        self.vol_df.to_json(self.fname, compression='gzip')
