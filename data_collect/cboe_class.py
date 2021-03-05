"""
CBOE Options Data Classes

#try:
#    self.df = self.df[~self.df['Underlying'].isin(etf_list['symbol'])]
#except KeyError:  # To avoid the 'symbol' error
"""

# %% codecell
##############################################################
import os
import json
from json import JSONDecodeError
from io import StringIO, BytesIO
import glob
import importlib
import sys
import copy
import zlib

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path
import base64

import datetime
from datetime import date, timedelta
import pytz

from charset_normalizer import CharsetNormalizerMatch
from charset_normalizer import detect
from charset_normalizer import CharsetNormalizerMatches as CnM

import xml.etree.ElementTree as ET

try:
    from scripts.dev.data_collect.options import DerivativesHelper
    from scripts.dev.data_collect.iex_class import readData
    importlib.reload(sys.modules['scripts.dev.data_collect.iex_class'])
    from scripts.dev.data_collect.iex_class import readData
    from scripts.dev.data_collect.help_class import baseDir, dataTypes
except ModuleNotFoundError:
    from data_collect.options import DerivativesHelper
    from data_collect.iex_class import readData
    from multiuse.help_class import baseDir, dataTypes

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', None)


# %% codecell
##############################################################

class cleanMmo():
    """Clean mmo data."""

    def __init__(self, mmo):
        self.date = mmo.date
        self.df = mmo.comb_df.copy(deep=True)
        self.nopop_top_2000 = self.process_data(self, mmo)
        print('xxx.nopop_top_2000')

    @classmethod
    def process_data(cls, self, mmo):
        """Pre process data and call next function in line."""
        # Delete unsused dataframes
        try:
            del mmo.mmo_df
            del mmo.sym_df
        except AttributeError:
            pass
        self._convert_cols(self)
        self._exclude_pop_symbols(self)
        self._total_volume_calc(self)
        self._conver_cols_nopop(self)
        self._create_exp_col(self)
        nopop_top_2000 = self._create_vol_sum(self)
        nopop_top_2000 = self._get_daily_difference(self, nopop_top_2000)
        self.exp_dict = self._create_time_frames(self)
        time_dict = self._filter_nopop_by_time_frame(self, nopop_top_2000)
        self._write_to_json(self, nopop_top_2000, time_dict)

        return nopop_top_2000

    @classmethod
    def _convert_cols(cls, self):
        """Convert columns to categories."""
        cols_to_cat = ['Symbol', 'Underlying', 'exchange', 'OSI Symbol', 'side']
        self.df[cols_to_cat] = self.df[cols_to_cat].astype('category')
        self.df.drop(columns=['Symbol', 'OSI Symbol'], inplace=True)
        self.df.rename(columns=({'Missed Liquidity': 'miss_liq',
                                 'Exhausted Liquidity': 'exh_liq',
                                 'Routed Liquidity': 'rout_liq',
                                 'Volume Opportunity': 'vol_opp',
                                 'expirationDate': 'expDate',
                                 'Liquidity Opportunity': 'liq_opp'}
                                ), inplace=True)

    @classmethod
    def _exclude_pop_symbols(cls, self):
        """Exclude popular symbols from the analysis."""
        etf_list = readData.etf_list()
        self.df = self.df[~self.df['Underlying'].isin(etf_list.values.ravel())]
        self.df.reset_index(inplace=True, drop=True)

        pop_syms = ['AAPL', 'AMZN', 'BBBY', 'TSLA', 'GOOGL']
        self.df = self.df[~self.df['Underlying'].isin(pop_syms)].copy(deep=True)

    @classmethod
    def _total_volume_calc(cls, self):
        """Calc total volume."""
        self.df['totVol'] = (abs(self.df['miss_liq']) +
                             abs(self.df['exh_liq']) +
                             abs(self.df['rout_liq']))

    @classmethod
    def _conver_cols_nopop(cls, self):
        """Convert columns from not popular data frame."""
        self.df = dataTypes(self.df).df

    @classmethod
    def _create_exp_col(cls, self):
        """Create expriration date column."""
        yr_fr = self.df['yr'].astype(str).str.rjust(3, fillchar='0')
        yr_fr = yr_fr.str.rjust(4, fillchar='2')
        mo_fr = self.df['mo'].astype(str).str.rjust(2, fillchar='0')
        day_fr = self.df['day'].astype(str).str.rjust(2, fillchar='0')

        self.df['expDate'] = yr_fr + '-' + mo_fr + '-' + day_fr
        self.df['expDate'] = self.df['expDate'].astype('category')

    @classmethod
    def _get_daily_difference(cls, self, nopop_top_2000):
        """Get the difference between data today and data yesterday."""
        top_fpaths = glob.glob(f"{baseDir().path}/derivatives/cboe/*")
        top_fpaths = sorted(top_fpaths)[:-1]
        last_df = pd.read_json(top_fpaths[-2], compression='gzip')

        mod_df = pd.merge(nopop_top_2000, last_df, how='outer', indicator='Exist')
        mod_df = mod_df[mod_df['Exist'] == 'left_only'].copy(deep=True)
        mod_df.drop(columns=['Exist'], inplace=True)
        mod_df.reset_index(inplace=True, drop=True)
        mod_df['dataDate'] = top_fpaths[-1][-13:-3]

        return mod_df

    @classmethod
    def _create_vol_sum(cls, self):
        """Create groupby object for symbol, exp date, side."""
        self.df['liq_opp'].fillna(0, inplace=True)
        cols_to_drop = (['miss_liq', 'exh_liq', 'rout_liq',
                         'strike', 'yr', 'mo', 'day'])
        exist_columns = self.df.columns
        cols_to_include = exist_columns[~exist_columns.isin(cols_to_drop)]
        nopop_vol_sums = self.df[cols_to_include].copy(deep=True)

        nopop_vol_sums = nopop_vol_sums.groupby(by=['expDate', 'Underlying', 'side']).sum()
        nopop_vol_sums.dropna(inplace=True)
        nopop_vol_sums.reset_index(inplace=True)

        nopop_vol_sums['vol/avg'] = (nopop_vol_sums['totVol'] /
                                     nopop_vol_sums['Cboe ADV']).astype(np.float16)

        # Get top 2000 for vol/avg
        nopop_top_2000 = nopop_vol_sums.sort_values(by=['vol/avg', 'liq_opp'], ascending=False).head(2000)
        return nopop_top_2000

    @classmethod
    def _create_time_frames(cls, self):
        """Create short-medium-long term time frames."""
        exp_dates = pd.to_datetime(self.df['expDate'].value_counts().index, yearfirst=True).date
        # Create empty dict to store dates
        exp_dict = {}

        exp_dict['short'] = exp_dates[exp_dates < (date.today() + timedelta(days=45))].astype(str)
        exp_dict['med'] = (exp_dates[(exp_dates > (date.today() + timedelta(days=45))) &
                                     (exp_dates < (date.today() + timedelta(days=180)))]).astype(str)
        exp_dict['long'] = exp_dates[exp_dates > (date.today() + timedelta(days=180))].astype(str)

        return exp_dict

    @classmethod
    def _filter_nopop_by_time_frame(cls, self, nopop_top_2000):
        """Filter non popular symbols by short, medium, and long term frames."""
        cols_to_float16 = nopop_top_2000.dtypes[nopop_top_2000.dtypes == 'float64'].index.to_list()
        nopop_top_2000[cols_to_float16] = nopop_top_2000[cols_to_float16].astype(np.float16)
        nopop_top_2000.reset_index(inplace=True, drop=True)

        # Create empty dict to store short-medium-long term dataframes
        time_dict = {}
        time_dict['short'] = nopop_top_2000[nopop_top_2000['expDate'].isin(self.exp_dict['short'])]
        time_dict['medium'] = nopop_top_2000[nopop_top_2000['expDate'].isin(self.exp_dict['med'])]
        time_dict['long'] = nopop_top_2000[nopop_top_2000['expDate'].isin(self.exp_dict['long'])]

        return time_dict

    @classmethod
    def _write_to_json(cls, self, nopop_top_2000, time_dict):
        """Write to local json file."""
        base_path = f"{baseDir().path}/derivatives/cboe"
        nopop_fname = f"{base_path}/nopop_2000_{self.date}.gz"

        if os.path.isfile(nopop_fname):
            pass
        else:
            nopop_top_2000.to_json(nopop_fname, compression='gzip')

        # Short-medium-long term data frames to json
        for t in time_dict.keys():
            time_fname = f"{base_path}/syms_to_explore/{t}_{self.date}.gz"
            self._if_file_exists(self, time_fname, time_dict, t)

    @classmethod
    def _if_file_exists(cls, self, fname, time_dict, t):
        """Check if file exists and don't overwrite if it does."""
        if os.path.isfile(fname):
            pass
        else:
            time_dict[t].to_json(fname, compression='gzip')



# %% codecell
##############################################################


# %% codecell
##############################################################
class cboeData():
    """Read/write/get cboe symbol reference/other data."""
    cboe_ex_list = ['cone', 'opt', 'ctwo', 'exo']
    base_dir = f"{baseDir().path}/derivatives"
    # 'mmo' = market maker opportunity
    # Data is stored in self.comb_df for combined dataframes

    def __init__(self, report):
        self.fname, self.sym_fname = '', ''
        self.mmo_df, self.sym_df, self.comb_df = '', '', ''
        self.date = self.date_to_use(self)

        if report == 'mmo':
            self.mmo(self)
        else:
            print("Report keyword does not match. Available are: 'mmo'")

    @classmethod
    def mmo(cls, self):
        """Market maker opportunity."""

        self.fname = f"{self.base_dir}/mmo/{self.date}.gz"

        if os.path.isfile(self.fname):
            self.comb_df = pd.read_json(self.fname)
        else:
            self.mmo_df = self.get_mmo_data(self)
            self.sym_df = self.read_symref(self)
            self.comb_df = self.merge_dfs(self)
            self.write_to_json(self)

    @classmethod
    def date_to_use(cls, self):
        """Get the right date to use."""
        nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
        nyc_hm = nyc_datetime.hour + (nyc_datetime.minute/60)
        cutoff_hm = 16.30
        # While current hh.mm < cuttoff
        if nyc_hm < cutoff_hm:
            self.date = DerivativesHelper.which_fname_date()
        else:
            self.date = date.today()
        return self.date

    @classmethod
    def get_mmo_data(cls, self):
        """Get current market making opp. data."""
        mm_dict = {}
        mm_url_p1 = "https://www.cboe.com/us/options"
        mm_url_p2 = "/market_statistics/maker_report/csv/?book=all&mkt="
        for ex in self.cboe_ex_list:
            mm_dict[ex] = (requests.get(f"{mm_url_p1}{mm_url_p2}{ex}")).content

        for ex in self.cboe_ex_list:
            mm_dict[ex] = pd.read_csv(
                        BytesIO(mm_dict[ex]),
                        escapechar='\n',
                        delimiter=',',
                        delim_whitespace=False,
                        skipinitialspace=False
                        )
        # Return concactenated dataframes
        df = self.concat_dfs(self, mm_dict)
        return df

    @classmethod
    def concat_dfs(cls, self, df_dict):
        """Concat exchange dfs into one."""
        # Create an empty dataframe
        df = pd.DataFrame()
        # Loop through dict and concat
        for ex in self.cboe_ex_list:
            df_dict[ex]['exchange'] = ex
            df = pd.concat([df, df_dict[ex]])
        # Delete the df_dict
        del df_dict
        # Reset the index
        df.reset_index(inplace=True, drop=True)
        # Return the dataframe
        return df

    @classmethod
    def read_symref(cls, self):
        """Symbol reference data."""
        self.sym_fname = f"{self.base_dir}/cboe_symref/symref_{self.date}.gz"

        if os.path.isfile(self.sym_fname):
            try:
                sym_df = pd.read_json(self.sym_fname)
            except zlib.error:
                sym_df = self.get_symref(self)
        else:
            sym_df = self.get_symref(self)

        return sym_df

    @classmethod
    def get_symref(cls, self):
        """Request symref data from CBOE."""
        sym_u1 = "https://www.cboe.com/us/options/market_statistics/symbol_reference/?mkt="
        sym_u2 = "&listed=1&unit=1&closing=1"

        sym_url_dict = {}
        for ex in self.cboe_ex_list:
            sym_url_dict[ex] = f"{sym_u1}{ex}{sym_u2}"

        symref_dict = {}
        for ex in self.cboe_ex_list:
            symref_dict[ex] = (requests.get(sym_url_dict[ex])).content

        sym_df = self.symref_to_pd(self, symref_dict)
        return sym_df


    @classmethod
    def symref_to_pd(cls, self, symref_dict):
        """Convert bytes to pandas dataframe."""
        symref_df_dict = {}

        for ex in self.cboe_ex_list:
            symref_df_dict[ex] = pd.read_csv(
                        BytesIO(symref_dict[ex]),
                        escapechar='\n',
                        delimiter=',',
                        delim_whitespace=False,
                        skipinitialspace=False
                        )
        sym_df = self.concat_dfs(self, symref_df_dict)
        sym_df = self.symref_format(self, sym_df)
        return sym_df

    @classmethod
    def symref_format(cls, self, df):
        """Format symbol reference data."""

        df['sym_suf'] = df['OSI Symbol'].str[-15:]
        df['side'] = df['sym_suf'].str[6]
        df['strike'] = (df['sym_suf'].str[7:12] + '.' + df['sym_suf'].str[13]).astype('float16')
        df['expirationDate'] = df['sym_suf'].str[0:6]
        df.drop(columns=['Closing Only', 'Matching Unit', 'sym_suf'], inplace=True)

        df['yr'] = df['expirationDate'].str[0:2]
        df['mo'] = df['expirationDate'].str[2:4]
        df['day'] = df['expirationDate'].str[4:6]

        df.rename(columns={'Cboe Symbol': 'Symbol'}, inplace=True)

        cols_to_category = ['Symbol', 'Underlying', 'exchange']
        df[cols_to_category] = df[cols_to_category].astype('category')

        return df

    @classmethod
    def merge_dfs(cls, self):
        """Merge mmo and symref dataframes."""
        try:
            df = pd.merge(self.mmo_df, self.sym_df, on=['Symbol', 'exchange', 'Underlying'], how='inner')
            df.reset_index(inplace=True, drop=True)
            df['rptDate'] = date.today()

            # Change data types to reduce file size
            cols_to_float16 = ['strike', 'Liquidity Opportunity']
            cols_to_int16 = (['Missed Liquidity', 'Exhausted Liquidity',
                              'Routed Liquidity', 'Volume Opportunity',
                              'expirationDate', 'Cboe ADV', 'yr', 'mo', 'day'])
            df[cols_to_float16] = df[cols_to_float16].astype(np.float16)
            df[cols_to_int16] = df[cols_to_int16].astype(np.uint16)
        except TypeError:
            df = pd.DataFrame()
        return df

    @classmethod
    def write_to_json(cls, self):
        """Write symref and mmo data to local json file."""
        self.sym_df.to_json(self.sym_fname, compression='gzip')
        # Write directly to the mmo directory
        self.comb_df.to_json(self.fname, compression='gzip')



# %% codecell
##############################################################

class cboeLocalRecDiff():
    """Get the recursive differences between CBOE data/days."""
    base_dir = f"{baseDir().path}/derivatives/cboe"
    base_url = "https://algotrading.ventures/api/v1/"
    top_2000 = "cboe/mmo/top"
    # url_suf = url_suffix
    # If fresh == True, get fresh data from API. If false, read local data

    def __init__(self, which, fresh):
        self.df = ''
        if fresh:
            self.cboe_dict = self.get_data(self, which)
            self.make_fname_lists(self)
            self.unmerged_df = self.convert_to_dataframe(self)
            self.df = self.clean_sort_write(self)
        else:
            self.df = self.read_local_data(self)

    @classmethod
    def read_local_data(cls, self):
        """Get all local files, read, concat, and return df."""
        # Top fpaths is all local cboe top 2000 symbols, minus dir
        top_fpaths = glob.glob(f"{self.base_dir}/*")
        top_fpaths = sorted(top_fpaths)[:-1]

        top_df = pd.DataFrame()
        for fs in top_fpaths:
            new_df = pd.DataFrame(pd.read_json(top_fpaths[fs], compression='gzip'))
            top_df = pd.concat([top_df, new_df])

        return top_df

    @classmethod
    def get_data(cls, self, which):
        url_suf = ''
        if which == 'top_2000':
            url_suf = self.top_2000

        cboe_url = f"{self.base_url}{url_suf}"
        cboe_get = requests.get(cboe_url)
        cboe_dict = json.load(BytesIO(cboe_get.content))

        return cboe_dict

    @classmethod
    def make_fname_lists(cls, self):
        """Make server and local fname lists."""
        local_fname_list = []
        for f in self.cboe_dict:
            local_fname_list.append(f"{baseDir().path}{f.split('data')[1]}")

        server_fname_list = []
        for f in self.cboe_dict:
            server_fname_list.append(f)

        self.local_flist = local_fname_list
        self.server_flist = server_fname_list

    @classmethod
    def convert_to_dataframe(cls, self):
        """Convert data to dataframe, add report date."""
        all_df = pd.DataFrame()

        for fs in self.cboe_dict:
            self.cboe_dict[fs] = pd.DataFrame(self.cboe_dict[fs])
            self.cboe_dict[fs]['dataDate'] = fs[-13:-3]
            all_df = pd.concat([all_df, self.cboe_dict[fs]])

        return all_df

    @classmethod
    def clean_sort_write(cls, self):
        """Clean and sort data."""
        flist = self.server_flist
        cboe = self.cboe_dict
        on_list = list(self.unmerged_df.columns.drop('dataDate'))

        top_df = pd.DataFrame()
        for fsn, fs in enumerate(flist):
            try:
                # print(flist[(fsn - 1)])
                df = pd.merge(cboe[flist[fsn]], cboe[flist[(fsn-1)]], how='outer', on=on_list, indicator='Exist').copy(deep=True)
                df = df[df['Exist'] == 'left_only'].copy(deep=True)
                df.drop(columns=['Exist'], inplace=True)
                df.reset_index(inplace=True, drop=True)
                df['dataDate'] = fs[-13:-3]
                top_df = pd.concat([top_df, df]).copy(deep=True)
                df.to_json(self.local_flist[fsn], compression='gzip')
            except IndexError:
                pass
        # Drop columns from merge
        top_df.drop(columns=['dataDate_x', 'dataDate_y'], inplace=True)
        return top_df

# %% codecell
##############################################################
