"""Functions for processing SEC Data."""

# %% codecell
################################################

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError
import time
import os.path
from datetime import date
import datetime
from io import BytesIO

import requests
import pandas as pd
from pandas.errors import EmptyDataError
import numpy as np
from bs4 import BeautifulSoup

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet
    from scripts.dev.multiuse.sec_helpers import make_sec_cik_ref
    from scripts.dev.multiuse.bs4_funcs import bs4_child_values
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet
    from multiuse.sec_helpers import make_sec_cik_ref
    from multiuse.bs4_funcs import bs4_child_values

# %% codecell
################################################


def form_4(get=False, url=False):
    """Get form 4 data and return dataframe."""
    if url:
        get = requests.get(url)
    elif get:
        pass
    split_test_xml = (get.content.split(
                        bytes("<XML>", 'utf-8')
                        )[1].decode('utf-8'))
    split_test_xml = split_test_xml.split('</XML>')[0]
    string = split_test_xml.strip("\n\t ")
    string = string.replace('\n', '')
    split_test_root = ET.fromstring(string)

    # Start with the value items to get the parents,
    # then assign the values to the parents
    all_value_tags = split_test_root.findall(".//value")
    all_value_parents = split_test_root.findall(".//value/..")
    all_elements = split_test_root.findall(".//")

    tag_dict = {}
    for tag, val in zip(all_value_parents, all_value_tags):
        tag_dict[tag.tag] = val.text

    mod_elements = ([elm for elm in all_elements if elm not in all_value_tags
                     if elm not in all_value_parents])
    for elm in mod_elements:
        try:
            if '}' in elm.tag:
                tag_dict[elm.tag.split('}')[1]] = elm.text.strip()
            else:
                tag_dict[elm.tag] = elm.text.strip()
        except AttributeError:
            print(elm.tag)

    df = pd.DataFrame(tag_dict, index=range(1))

    return df


def form_8k(get):
    """Get form 4 data and return dataframe."""
    # test_4_get = requests.get(url)
    split_xml = get.content.split(bytes("<XML>", 'utf-8'))
    split_xml_close = get.content.split(bytes("</XML>", 'utf-8'))

    split_xml = [sp.decode('utf-8') for sp in split_xml]
    split_xml_close = [sp.decode('utf-8') for sp in split_xml_close]
    all_df = pd.DataFrame()

    for sp in range(1, len(split_xml)):
        split_xml = split_xml[sp]
        split_test_xml = split_xml.split('</XML>')[sp]
        string = split_test_xml.strip("\n\t ")
        string = string.replace('\n', '')
        try:
            split_test_root = ET.fromstring(string)
        except ParseError:
            print(string)
            break

        # Start with the value items to get the parents,
        # then assign the values to the parents
        all_value_tags = split_test_root.findall(".//value")
        all_value_parents = split_test_root.findall(".//value/..")
        all_elements = split_test_root.findall(".//")

        tag_dict = {}
        for tag, val in zip(all_value_parents, all_value_tags):
            tag_dict[tag.tag] = val.text

        mod_elements = ([elm for elm in all_elements if elm not in all_value_tags
                         if elm not in all_value_parents])
        for elm in mod_elements:
            try:
                if '}' in elm.tag:
                    tag_dict[elm.tag.split('}')[1]] = elm.text.strip()
                else:
                    tag_dict[elm.tag] = elm.text.strip()
            except AttributeError:
                print(elm.tag)

        df = pd.DataFrame(tag_dict, index=range(1))
        all_df = pd.concat([all_df, df])

    return all_df

# %% codecell
################################################


class secCompanyIdx():
    """Get master parquet list from sec edgar."""

    # Store as local dataframe. Accepts either symbol or cik #
    base_dir = baseDir().path

    def __init__(self, sym=False, cik=False):
        self.construct_params(self, sym, cik)
        self.retrieve_data(self)
        self.write_or_update(self)

    @classmethod
    def construct_params(cls, self, sym, cik):
        """Construct url and local fpath."""
        all_syms_fpath = f"{self.base_dir}/tickers/all_symbols.parquet"
        all_symbols = pd.read_parquet(all_syms_fpath)
        # Drop cik values that are NaNs or infinite
        all_symbols.dropna(axis=0, subset=['cik'], inplace=True)
        all_symbols['cik'] = all_symbols['cik'].astype(np.uint32)

        if sym:  # Get symbol cik number for edgar lookup
            cik = (all_symbols[all_symbols['symbol'] == sym]
                   .head(1)['cik'].astype('uint32').iloc[0])
        elif cik:  # Get symbol
            sym = (all_symbols[(all_symbols['cik'] == cik)
                   & (all_symbols['type'] == 'cs')])

        # Construct local fpath
        self.fpath = f"{self.base_dir}/sec/company_index/{str(cik)[-1]}/_{cik}.parquet"
        # Sec base url
        sec_burl = 'https://data.sec.gov/submissions/CIK'
        self.url = f"{sec_burl}{str(cik).zfill(10)}.json"
        # cik and sym
        self.sym, self.cik = sym, cik

    @classmethod
    def retrieve_data(cls, self):
        """Get data from SEC EDGAR and convert to parquet."""
        sec_get = requests.get(self.url)
        if sec_get.status_code != 200:
            time.sleep(1)  # Sleep for 1 second and retry
            sec_get = requests.get(self.url)
        self.sec_df = pd.DataFrame(sec_get.json()['filings']['recent'])

    @classmethod
    def write_or_update(cls, self):
        """Write new file or update from previous."""
        if os.path.isfile(self.fpath):
            sec_prev_df = pd.read_parquet(self.fpath)
            sec_df = pd.concat([sec_prev_df, self.sec_df])
            sec_df.drop_duplicates(subset=['accesssionNumber'], inplace=True)
            self.sec_df = sec_df.reset_index(drop=True).copy(deep=True)

        write_to_parquet(self.sec_df, self.fpath)
        self.df = self.sec_df.copy(deep=True)

# %% codecell
################################################


class secMasterIdx():
    """Get sec master index daily after 5:30 pm."""

    # Sec base url
    sec_burl = 'https://www.sec.gov/Archives/edgar/daily-index'
    # Base master sec directory
    baster = f"{baseDir().path}/sec/daily_index"
    df, get_hist_date, url = False, False, False
    # hist_date is an optional param for a specific date
    # datetime.date or %Y%m%d formatted string
    headers = ({'User-Agent': 'Rogue Technology Ventures edward@rogue.golf',
                'Host': 'www.sec.gov',
                'Accept-Encoding': 'gzip, deflate',
                'Cache-Control': 'no-cache',
                'Accept-Language': 'en-GB,en;q=0.5'})

    def __init__(self, hist_date=False):
        self.determine_params(self, hist_date)
        if not isinstance(self.df, pd.DataFrame):
            self.retrieve_data(self)
            try:
                self.process_data(self)
                self.write_to_parquet(self)
            except KeyError:
                print(f"secMasterIdx: KeyError {self.url}")
            except EmptyDataError:
                print(f"secMasterIdx: EmptyDataError for url: {self.url}")

    @classmethod
    def determine_params(cls, self, hist_date):
        """Check for existing file or construct params."""
        if hist_date:
            self._read_existing_idx(self, hist_date)
        else:
            self._construct_params(self)

    @classmethod
    def _read_existing_idx(cls, self, hist_date):
        """Read existing index file with fpath."""
        yr = None

        if isinstance(hist_date, datetime.date):
            yr = hist_date.year
            hist_date = hist_date.strftime('%Y%m%d')
        else:
            yr = hist_date[0:4]

        self.fpath = f"{self.baster}/{yr}/_{hist_date}.parquet"
        self.fpath_raw = f"{self.baster}/{yr}/raw/_{hist_date}.parquet"
        # If local master file exists for that date
        if os.path.isfile(self.fpath):
            self.df = pd.read_parquet(self.fpath)
        else:
            self.get_hist_date = hist_date
            self._construct_params(self)

    @classmethod
    def _construct_params(cls, self):
        """Construct parameters for request, fpath."""
        if self.get_hist_date:
            # Convert historical date str to datetime.date
            hist_dt = (datetime.datetime.strptime(
                        self.get_hist_date, '%Y%m%d').date())
            yr = hist_dt.year
            # Financial quarter that we are currently in
            f_quart = f"QTR{str((hist_dt.month - 1) // 3 + 1)}"
            dt_fmt = self.get_hist_date
        else:
            dt = getDate.query('sec_master')
            yr = dt.year  # Year
            # Financial quarter that we are currently in
            f_quart = f"QTR{str((dt.month - 1) // 3 + 1)}"
            # Formatted year month day
            dt_fmt = dt.strftime('%Y%m%d')
        # Url suffix using the formatted date
        mast_suf = f"master.{dt_fmt}.idx"
        self.fpath = f"{self.baster}/{yr}/_{dt_fmt}.parquet"
        self.fpath_raw = f"{self.baster}/{yr}/raw/_{dt_fmt}.parquet"
        self.url = f"{self.sec_burl}/{yr}/{f_quart}/{mast_suf}"

    @classmethod
    def retrieve_data(cls, self):
        """Get SEC master index file."""
        get = requests.get(self.url, headers=self.headers)
        if get.status_code != 200:
            time.sleep(1)  # Sleep for 1 second and retry
            get = requests.get(self.url, headers=self.headers)
        self.get = get

    @classmethod
    def process_data(cls, self):
        """Process sec master index file."""
        df = (pd.read_csv(BytesIO(self.get.content),
                          delimiter='|',
                          escapechar='\n',
                          skiprows=5)).iloc[1:]
        df.dropna(axis=0, subset=['CIK'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['date'] = pd.to_datetime(df['Date Filed'], format='%Y%m%d', errors='coerce')
        df['CIK'] = df['CIK'].astype('str').str.zfill(10)
        df.rename(columns={'CIK': 'cik', 'Company Name': 'name'}, inplace=True)
        self.df = df

    @classmethod
    def write_to_parquet(cls, self):
        """Write dataframe to parquet."""
        write_to_parquet(self.df, self.fpath)

# %% codecell
##########################################################


class secInsiderTrans():
    """Get insider transactions from EDGAR."""

    # Base directory
    base_dir = baseDir().path
    # Sec base insiders transactions fpath
    base_insider = f"{base_dir}/sec/insider_trans"
    # Class variables
    df = False
    # Base url without cik
    sec_burl = "https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK="

    def __init__(self, sym=False, cik=False, refresh=False):
        self.determine_params(self, sym, cik, refresh)
        if not isinstance(self.df, pd.DataFrame):
            self.retrieve_data(self)
            self.process_data(self)
            self.write_to_parquet(self)

    @classmethod
    def determine_params(cls, self, sym, cik, refresh):
        """Determine parameters or read local."""
        if sym and not cik:  # Get CIK
            self.cik, self.sym = get_cik('TDAC').astype(np.uint32), sym
        elif cik and not sym:  # Get underlying symbll from ref data
            syms_fpath = f"{self.base_dir}/tickers/all_symbols.parquet"
            all_syms = pd.read_parquet(syms_fpath)
            sym_row = (all_syms[(all_syms['cik'] == cik)
                                & (all_syms['type'] == 'cs')].iloc[0])
            self.cik, self.sym = cik.astype(np.uint32), sym_row['symbol']

        # Check for existing file
        self.fpath = f"{self.base_insider}/{str(self.cik)[-1]}/_{self.cik}.parquet"

        if os.path.isfile(self.fpath) and not refresh:
            self.df = pd.read_parquet(self.fpath)
        else:  # If not file, construct url with CIK filled to 10 digits
            self.url = f"{self.sec_burl}{str(self.cik).zfill(10)}"

    @classmethod
    def retrieve_data(cls, self):
        """Retrieve data from sec EDGAR database."""
        get = requests.get(self.url)
        if get.status_code != 200:
            time.sleep(1)
            get = requests.get(self.url)
        self.get = get

    @classmethod
    def process_data(cls, self):
        """Process insider transaction data."""
        soup = BeautifulSoup(self.get.content.decode('utf-8'), 'html.parser')
        tbl = soup.find("table", id='transaction-report')

        # Get column headers
        col_list = (bs4_child_values(tbl, elm="tr",
                                     class_='header', cols=True))
        # Extract insider transaction data
        data_list = bs4_child_values(tbl, elm="tr", class_='')
        # Create dataframe
        self.df = pd.DataFrame(data_list, columns=col_list)

    @classmethod
    def write_to_parquet(cls, self):
        """Write dataframe to parquet."""
        write_to_parquet(self.df, self.fpath)
