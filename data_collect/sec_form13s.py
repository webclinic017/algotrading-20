"""Classes for processing form 13s."""
# %% codecell
#################################################
import os
import time
import xml.etree.ElementTree as ET

import requests
import pandas as pd
import numpy as np


try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

# %% codecell
#################################################


class get13F():
    """Get ind. quarterly inst. holdings."""

    base_dir = baseDir().path
    # Sec base url
    sec_url = "https://www.sec.gov/Archives"
    # Variables
    df = False
    # Row data is passed to __init__ function

    def __init__(self, row, delay=False):
        self.make_params(self, row)
        if delay:
            time.sleep(.25)
        if not isinstance(self.df, pd.DataFrame):
            self.retrieve_data(self)
            self.process_data(self)
            self.add_cik(self)
            self.write_to_json(self)

    @classmethod
    def make_params(cls, self, row):
        """Make local fpath params. See if file exists."""
        # Create empty variable
        fpath_quart = ''
        # If row is dict, convert to Series
        if isinstance(row, dict):
            row = pd.Series(row)
        # Get datetime of row data
        row_dt = (pd.to_datetime(row['Date Filed'],
                                 format='%Y%m%d').date())
        f_cik, f_file = row['CIK'], row['Form Type']
        # Define CIK that will be added to the dataframe later
        self.cik = row['CIK']
        f_quart = f"Q{str((row_dt.month - 1) // 3 + 1)}"

        # Construct local fpaths for file/dirs
        fpath_base = f"{self.base_dir}/sec/institutions/{row_dt.year}"
        try:
            fpath_quart = f"{fpath_base}/{f_cik[-1]}/_{f_cik}/{f_quart}"
        except IndexError:
            fpath_quart = f"{fpath_base}/{f_cik.astype('str')[-1]}/_{f_cik}/{f_quart}"
        self.fpath_full = f"{fpath_quart}/_{f_file}.gz"

        # Make full fpath directory
        if not os.path.isdir(fpath_quart):
            os.umask(0)
            os.makedirs(fpath_quart, mode=0o777)

        if os.path.isfile(self.fpath_full):
            self.df = pd.read_json(self.fpath_full, compression='gzip')
        else:
            self.url = f"{self.sec_url}/{row['File Name']}"

    @classmethod
    def retrieve_data(cls, self):
        """Get data from sec edgar."""
        get = requests.get(self.url)
        if get.status_code != 200:
            time.sleep(1)
            get = requests.get(self.url)
        if get.status_code != 200:
            time.sleep(1)
            get = requests.get(self.url)
        self.get = get

    @classmethod
    def process_data(cls, self):
        """Convert to xml, extract columns, data."""
        get_str = self.get.content.decode('utf-8')
        # Split string to second XML section
        split_str = (get_str.split('<XML>')[2].split('</XML>')[0]
                            .strip('\n\t ').replace('\n', ' '))
        # Convert to XML root
        str_root = ET.fromstring(split_str)
        all_elements = str_root.findall(".//")

        col_list = self._create_col_tags(all_elements)
        self.df = self._extract_data(all_elements, col_list)

    @classmethod
    def _create_col_tags(cls, all_elements):
        """Create a list of column tags."""
        # We can create a list of column tags
        unmod_col_list, col_list = [], []
        for elm in all_elements:
            if elm.tag not in unmod_col_list:
                if '}' in elm.tag:
                    col_list.append(elm.tag.split('}')[1])
                else:
                    col_list.append(elm.tag)
                unmod_col_list.append(elm.tag)
            else:
                break
        return col_list

    @classmethod
    def _extract_data(cls, all_elements, col_list):
        """For loop to pull string attributes out of data."""
        n_new_row = list(range(len(col_list)))[-1]
        row_dict = dict([(row, '') for row in col_list])
        data = []
        for eln, elm in enumerate(all_elements):
            if eln % n_new_row == 0:
                data.append(row_dict)
            elif (eln + 1) % n_new_row == 0:
                row_dict = dict([(row, '') for row in col_list])
            else:
                pass
            try:
                if '}' in elm.tag:
                    row_dict[elm.tag.split('}')[1]] = elm.text.strip()
                else:
                    row_dict[elm.tag] = elm.text.strip()
            except AttributeError:
                print(elm.tag)

        df = pd.DataFrame(data)
        df.replace('', np.nan, regex=False, inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        return df

    @classmethod
    def add_cik(cls, self):
        """Add CIK to dataframe."""
        self.df['CIK'] = self.cik

    @classmethod
    def write_to_json(cls, self):
        """Write dataframe to json."""
        self.df.to_json(self.fpath_full, compression='gzip')
