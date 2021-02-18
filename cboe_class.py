"""
CBOE Options Data Classes
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

from pandas_df_options import freeze_header

import xml.etree.ElementTree as ET

from options import DerivativesHelper
from theocc_class import TradeVolume

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', None)

from pyinstrument import Profiler
profiler = Profiler()

# %% codecell
##############################################################

# The thesis is trying to find names that previously had liquidity and now don't
# From one week to another, this will give a good idea of where market makers are
# Forced to buy more options
"""
try:
    df = pd.merge(self.mmo_df, self.sym_df, on=['Symbol', 'exchange', 'Underlying'], how='left')
    df.reset_index(inplace=True, drop=True)
except TypeError:
    df = pd.DataFrame()
return df
"""


mmo = cboeData('mmo')

mmo_df = mmo.mmo_df.copy(deep=True)
sym_df = mmo.sym_df.copy(deep=True)
mmo_df.shape
sym_df.shape

# Keep only the next few years
years_to_get = [20, 21, 22, 23, 24, 25]
sym_df = sym_df[sym_df['yr'].isin(years_to_get)]

df = pd.merge(mmo_df, sym_df, on=['Symbol', 'exchange', 'Underlying'], how='inner')

df.shape

df.info(memory_usage='deep')

cols_to_float4 = ['strike', 'Liquidity Opportunity']
cols_to_int = ['Missed Liquidity', 'Exhausted Liquidity', 'Routed Liquidity', 'Volume Opportunity', 'expirationDate', 'Cboe ADV', 'yr', 'mo', 'day']
df[cols_to_float4] = df[cols_to_float4].astype(np.float16)
df[cols_to_int] = df[cols_to_int].astype(np.int8)

df.info(memory_usage='deep')

df.head(10)

sym_df.shape

mmo_df.head(2)

mmo_df.info(memory_usage='deep')




# %% codecell
##############################################################
mmo_only = mmo.mmo_df.copy(deep=True)
sym_only = mmo.sym_df.copy(deep=True)



report_date = datetime.date(2021, 2, 16)
td_vol_df = TradeVolume(report_date, 'con_volume')
td_vol_df = td_vol_df.vol_df

cols_to_int = ['firmQuantity', 'customerQuantity', 'marketQuantity']

td_vol_df[cols_to_int] = td_vol_df[cols_to_int].astype('int')

td_vol_df['totalVolume'] = td_vol_df['firmQuantity'] + td_vol_df['customerQuantity'] + td_vol_df['marketQuantity']

td_syms_df = td_vol_df[td_vol_df['underlying'].isin(mmo_syms)]

td_vol_df.dtypes

etfs_to_exclude = ['HYG', 'XLF', 'EEM', 'EWZ', 'TLT', 'VXX', 'RUT', 'ARKK', 'XLE', 'UVXY', 'EFA']
td_syms_df = td_syms_df[~td_syms_df['underlying'].isin(etfs_to_exclude)]

td_syms_df.sort_values(by='customerQuantity', ascending=False).head(200)

# Get memory usage
mmo_df2.info(memory_usage='deep')

# %% codecell
##############################################################

mmo_syms = mmo_df2['Underlying'].value_counts()[0:250].index.to_list()
len(mmo_syms)
len(mmo_syms)

mmo_syms
# .index.to_list()
# comb_df = pd.merge(mmo_df2, df_vol_df, on=['], how='left')
# mmo_df2['missedOpportunity'] = mmo_df2['Missed Liquidity'] / mmo_df2['Cboe ADV']

mmo_df2.shape


mmo_df2.sort_values(by=['Liquidity Opportunity'], ascending=False).head(200)
mmo_df2[mmo_df2['Underlying'] == 'TXMD'].head(50)
mmo_df2.sort_values(by=['Missed Liquidity'], ascending=False).head(50)
mmo_df2[mmo_df2['Underlying'] == 'XIN'].head(10)

mmo_df2.sort_values(by=['missedOpportunity'], ascending=False).head(50)
mmo_df2.sort_values(by=['Liquidity Opportunity'], ascending=False).head(50)

mmo_df2.rename(columns={'Underlying': 'underlying'}, inplace=True)


# %% codecell
##############################################################
profiler.start()
mmo_clean_df = cleanMmo(mmo)
profiler.stop()

print(profiler.output_text(unicode=True, color=True))
# %% codecell
##############################################################

class cleanMmo():
    """Clean mmo data."""

    def __init__(self, mmo):
        self.df = mmo.comb_df.copy(deep=True)
        self.process_data(self)

    @classmethod
    def process_data(cls, self):
        """Pre process data and call next function in line."""
        self.df.dropna(axis=0, inplace=True)
        self._exclude_pop_symbols(self)

    @classmethod
    def _exclude_pop_symbols(cls, self):
        """Exclude popular symbols from the analysis."""
        syms_to_exclude = (['SPY', 'QQQ', 'GLD', 'AAPL',
                            'AMZN', 'TSLA', 'PLTR', 'FB',
                            'TWTR', 'SPCE', 'IWM'])
        self.df = self.df[~self.df['Underlying'].isin(syms_to_exclude)]

        self._make_contractDate_col(self)

    @classmethod
    def _make_contractDate_col(cls, self):
        """Make a column for contract date."""
        cols_to_use = ['yr', 'mo', 'day']
        df_mod = self.df[cols_to_use].copy(deep=False)

        for col in cols_to_use:  # fr = formatted
            df_mod[f"{col}_fr"] = df_mod[col].astype('int').astype('str').str.zfill(2)

        self.df['contdate'] = f"20{df_mod['yr_fr']}-{df_mod['mo_fr']}-{df_mod['day_fr']}"

# %% codecell
##############################################################


# %% codecell
##############################################################
class cboeData():
    """Read/write/get cboe symbol reference/other data."""
    cboe_ex_list = ['cone', 'opt', 'ctwo', 'exo']
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives"
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
            sym_df = pd.read_json(self.sym_fname)
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

        # Keep only the next few years
        years_to_get = [20, 21, 22, 23, 24, 25]
        sym_df = sym_df[sym_df['yr'].isin(years_to_get)]
        return sym_df

    @classmethod
    def symref_format(cls, self, df):
        """Format symbol reference data."""
        df['sym_suf'] = df['OSI Symbol'].str[-15:]
        df['side'] = df['sym_suf'].str[6]
        df['strike'] = (df['sym_suf'].str[7:12] + '.' + df['sym_suf'].str[13]).astype('float16')
        df['expirationDate'] = df['sym_suf'].str[0:5]
        df.drop(columns=['Closing Only', 'Matching Unit', 'OSI Symbol', 'sym_suf'], inplace=True)

        df['yr'] = df['expirationDate'].str[0:2]
        df['mo'] = df['expirationDate'].str[2:4]
        df['day'] = df['expirationDate'].str[4:6]

        df.rename(columns={'Cboe Symbol': 'Symbol'}, inplace=True)

        return df

    @classmethod
    def merge_dfs(cls, self):
        """Merge mmo and symref dataframes."""
        try:
            df = pd.merge(self.mmo_df, self.sym_df, on=['Symbol', 'exchange', 'Underlying'], how='inner')
            df.reset_index(inplace=True, drop=True)

            # Change data types to reduce file size
            cols_to_float4 = ['strike', 'Liquidity Opportunity']
            cols_to_int = (['Missed Liquidity', 'Exhausted Liquidity',
                            'Routed Liquidity', 'Volume Opportunity',
                            'expirationDate', 'Cboe ADV', 'yr', 'mo', 'day'])
            df[cols_to_float4] = df[cols_to_float4].astype(np.float16)
            df[cols_to_int] = df[cols_to_int].astype(np.int8)
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
