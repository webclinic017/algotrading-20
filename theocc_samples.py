"""
Sample work for the occ
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

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path
import base64

import datetime
from datetime import date, timedelta

from charset_normalizer import CharsetNormalizerMatch
from charset_normalizer import detect
from charset_normalizer import CharsetNormalizerMatches as CnM

from data_collect.iex_class import expDates, marketHolidays
importlib.reload(sys.modules['data_collect.iex_class'])
from data_collect.iex_class import expDates, marketHolidays

from multiuse.help_class import dataTypes, getDate

from data_collect.theocc_class import tradeVolume, occFlex
importlib.reload(sys.modules['data_collect.theocc_class'])
from data_collect.theocc_class import tradeVolume, occFlex

import xml.etree.ElementTree as ET

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', None)



# %% codecell
##############################################################

url = 'https://marketdata.theocc.com/delo-download?prodType=ALL&downloadFields=OS;US;SN&format=txt'
get = requests.get(url)
dlp_df = pd.DataFrame(pd.read_csv(BytesIO(get.content)), escapechar='\n', delimiter='\t')
print(CnM.from_bytes(get.content).best().first())
get_sample = get.content[0:1000]
get_sample



"""
sym = 'IBM'
occ = requests.get(f"https://marketdata.theocc.com/series-search?symbolType=U&symbol={sym}")
occ_df = pd.read_csv(BytesIO(occ.content), skiprows=6, escapechar='\n', delimiter='\t')
cols = occ_df.columns[:-1]
occ_df.drop('year', inplace=True, axis=1)
occ_df.columns = cols



occ_df.head(5)
"""


# %% codecell
##############################################################
"""
voa_url1 = "https://marketdata.theocc.com/onn-volume-download?productKind=options&reportType=D"
voa_url2 = "&reportDate=20210212&reportFormat=both&issues=all&reportView=raw"

voa_get = requests.get(f"{voa_url1}{voa_url2}")

type(voa_get.content)
len(voa_get.content)

voa_nf = pd.read_csv(
            BytesIO(voa_get.content),
            escapechar='\n',
            delimiter=',',
            delim_whitespace=True,
            skipinitialspace=True
            )

voa_nf.head(100)
voa_sample = voa_get.content[0:10000]
print(CnM.from_bytes(voa_sample).best().first())
"""
# %% codecell
##############################################################
"""
report_date = datetime.date(2021, 2, 12)
oi_url = "https://marketdata.theocc.com/daily-open-interest?reportDate=20210212&action=download&format=csv"
oi_get = requests.get(oi_url)

len(oi_get.content)
type(oi_get.content)

oi_nf = pd.read_csv(
            BytesIO(oi_get.content),
            escapechar='\n',
            delimiter=',',
            delim_whitespace=True,
            skipinitialspace=True
            )

oi_nf.head(10)

report_date = datetime.date(2021, 2, 5)

report_date
otc_oi_url = f"https://marketdata.theocc.com/otc-open-interest?reportDate={report_date.strftime('%Y%m%d')}"
otc_oi_get = requests.get(otc_oi_url)
type(otc_oi_get.content)
len(otc_oi_get.content)

otc_oi_nf = pd.read_csv(
            BytesIO(otc_oi_get.content),
            escapechar='\n',
            delimiter=',',
            delim_whitespace=True,
            skipinitialspace=True
            )

otc_oi_nf.head(10)

print(CnM.from_bytes(otc_oi_get.content).best().first())



contract_url = f"https://marketdata.theocc.com/cont-volume-download?reportDate=20210212&format=xml"
contract_get = requests.get(contract_url)

len(contract_get.content)

root = ET.fromstring(contract_get.content.decode('ISO-8859-1'))

vol_dict = {}
# Column names for 9 unique cols
col_tags = [tg.tag for tg in root.findall('./record/*')[:10]]
for tg in col_tags:
    vol_dict[tg] = [tg.text for tg in root.findall(f"./record/{tg}")]
# Convert to dataframe
vol_df = pd.DataFrame(vol_dict)

dump_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/occ_dump"
f"{self.dump_dir}/vol/convol_{report_date}.json"

# 15 is set as the max number of possibly unique columns to look at
col_uni = list(set([tg.tag for tg in root.findall('./record/*')[:15]]))




vol_df.shape

vol_df.head(10)


con_sample = contract_get.content[0:10000]
print(CnM.from_bytes(con_sample).best().first())
"""
# %% codecell
##############################################################


exp = expDates('IBM').dates
exp


query_date = getDate.query(site='occ').strftime("%Y%m%d")
query_date
# %% codecell
######################################################################
occ_volume_og = pd.DataFrame()

#for sym in symbols:
for contract_date in exp:
    productKind = "ALL"
    accountType = "ALL"
    vol_url_1 = f"https://marketdata.theocc.com/volume-query?reportDate={query_date}&format=csv&volumeQueryType=O"
    vol_url_2 = f"&symbolType=U&symbol={symbol}&reportType=M&accountType={accountType}&productKind={productKind}&porc=BOTH&contractDt={contract_date}"
    occ_volume = requests.get(f"{vol_url_1}{vol_url_2}")
    occ_volume_df = pd.read_csv(BytesIO(occ_volume.content), escapechar='\n', delimiter=',')
    occ_volume_og = pd.concat([occ_volume_og, occ_volume_df])

# %% codecell
######################################################################
vol_df = occ_volume_og.copy(deep=True)

vol_df = dataTypes(vol_df).df
vol_df.rename(columns={'porc': 'side'}, inplace=True)
# vol_df.drop(columns=['underlying'], inplace=True)
cust_vol_df = vol_df[vol_df['actype'] == 'C']
firm_vol_df = vol_df[vol_df['actype'] == 'F']
mark_vol_df = vol_df[vol_df['actype'] == 'M']

vol_df_sum = vol_df.groupby(by=['contractDate', 'actdate', 'symbol', 'actype', 'side']).sum()

firm_vol_df.head(10)



actdates = vol_df['actdate'].value_counts().index.to_list()
len(actdates)

vol_df_sum.sort_values(by='contractDate', ascending=False).head(150)

vol_df.head(10)



occ_volume_og['contractDate'].value_counts()

cols_og = occ_volume_og.columns[1:-1]
occ_volume_og = occ_volume_og.reindex(cols_og, axis=1)

occ_volume_og.head(100)

occ_volume_firms = occ_volume_og[occ_volume_og['actype'] == 'F']
occ_volume_firms


# Load base_directory (derivatives data)
base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/theocc"
fname = f"{base_dir}/{symbol[0].lower()}/_{symbol}_{DerivativesHelper.which_fname_date() + timedelta(days=1)}"

# Reset the index inplace
occ_volume_og.reset_index(drop=True, inplace=True)
# Write to local json file
occ_volume_og.to_json(fname)

"""
# Get a list of all files in the base directory
choices = glob.glob(f"{base_dir}/*/*")

Remove all data in subdirectory!
for choice in choices:
    os.remove(choice)
"""

occ_volume_firms.head(50)

# sym_data = pd.concat([sym_data, pd.json_normalize(json.loads(rep.content))])

# occ_volume.content[0: 500]

occ_volume_df = pd.read_csv(BytesIO(occ_volume.content), escapechar='\n', delimiter=',')
occ_volume_df.shape

occ_volume_df['year'] = occ_volume_df['contractDate'].str[-4:]
occ_volume_df['expirationDate'] = occ_volume_df['contractDate'].str[0:5]
occ_volume_df['expirationDate'] = occ_volume_df['year'] + occ_volume_df['expirationDate']

occ_volume_df['expirationDate'] = occ_volume_df['expirationDate'].replace(regex='/', value='')


occ_volume_df['SYMBOL'] = occ_volume_df['symbol']
occ_volume_mr = occ_volume_df[occ_volume_df['actdate'] == (DerivativesHelper.which_fname_date() + timedelta(days=1)).strftime('%m/%d/%Y')]

occ_volume_mr.head(10)

occ_volume_df.shape
occ_volume_df.head(100)

# %% codecell
############################################################
# Contract volume is just all the trades that were made on that day
# For all symbols, for all expiration dates

query_date = getDate.query(site='occ')
con_vol = TradeVolume(query_date, 'con_volume')
con_vol_df = con_vol.vol_df.copy(deep=True)

con_vol_df['pkind'].value_counts()

con_vol_df.head(10)


# %% codecell
############################################################
"""
ess_url = f"https://marketdata.theocc.com/ess-reports?reportDate=20210212"
ess = requests.get(ess_url)

print(CnM.from_bytes(ess.content).best().first())

thresh_url = f"https://marketdata.theocc.com/threshold-securities?reportDate=20210217"
thresh_sec = requests.get(thresh_url)

print(CnM.from_bytes(thresh_sec.content).best().first())
"""
# %% codecell
######################################################################

monthly_url = "https://marketdata.theocc.com/weekly-volume-reports?reportDate=20210212&reportType=options&reportClass=equity&format=csv"
monthly_get = requests.get(monthly_url)

len(monthly_get.content)
type(monthly_get.content)

monthly_sample = monthly_get.content
print(CnM.from_bytes(monthly_sample).best().first())

mon_df = pd.read_csv(
            BytesIO(monthly_get.content),
            escapechar='\n',
            delimiter=',',
            delim_whitespace=True,
            skipinitialspace=True
            )


mon_df.head(10)
# %% codecell
######################################################################



"""

oi_url = "https://marketdata.theocc.com/daily-open-interest?reportDate=02/12/2021&action=download&format=csv"
oi_get = requests.get(oi_url)

print(len(oi_get.content))

print(CnM.from_bytes(oi_get.content).best().first())

# %% codecell
######################################################################
# Position limits
dlp_url = "https://marketdata.theocc.com/delo-download?prodType=ALL&downloadFields=OS;US;SN;EXCH;PL;ONN&format=txt"
dlp_get = requests.get(dlp_url)
print(len(dlp_get.content))

dlp_sample = dlp_get.content[0:10000]

print(CnM.from_bytes(dlp_sample).best().first())


# %% codecell
######################################################################

ex_vol_reg_url = "https://marketdata.theocc.com/exchange-volume?reportView=regular&reportType=D&reportDate=20210212&instrumentType=options&format=txt"
ex_vol_reg_get = requests.get(ex_vol_reg_url)

print(len(ex_vol_reg_get.content))

print(CnM.from_bytes(ex_vol_reg_get.content).best().first())

# %% codecell
######################################################################

ex_vol_mark_url = "https://marketdata.theocc.com/exchange-volume?reportView=market&reportType=D&reportDate=20210212&instrumentType=options&format=txt"
ex_vol_mark_get = requests.get(ex_vol_mark_url)

print(len(ex_vol_mark_get.content))

print(CnM.from_bytes(ex_vol_mark_get.content).best().first())


# %% codecell
######################################################################
otc_oi_url = "https://marketdata.theocc.com/otc-open-interest?reportDate=20210205"
otc_oi_get = requests.get(otc_oi_url)

print(len(otc_oi_get.content))

print(CnM.from_bytes(otc_oi_get.content).best().first())

# %% codecell
######################################################################
# Get all stocks series

series_url = "https://marketdata.theocc.com/series-download?exchanges=02,04,26,18,25,01,35,22,08,55,19,16,12,11,03,20,27,13,07&downloadType=D&dates=02/12/2021"
series_get = requests.get(series_url)

print(len(series_get.content))

series_sample = series_get.content[0:10000]

print(CnM.from_bytes(series_sample).best().first())
"""


# %% codecell
######################################################################
vol_sym_url_1 = "https://marketdata.theocc.com/volume-query?reportDate=20210212&format=csv&volumeQueryType=O"
vol_sym_url_2 = "&symbolType=ALL&symbol=ALL&reportType=D&accountType=ALL&productKind=ALL&porc=BOTH&contractDt=20210219"

vol_sym_url = f"{vol_sym_url_1}{vol_sym_url_2}"
vol_sym_get = requests.get(vol_sym_url)

print(len(vol_sym_get.content))

# %% codecell
######################################################################


# occ_comb = pd.merge(occ_volume_mr, mr_stocks[['symbol', 'fClose']], on='symbol', how='left')

# print(CnM.from_bytes(flex_get.content).best().first())

# %% codecell
######################################################################
# Stock loan report
https://marketdata.theocc.com/mdapi/stock-loan?report_date=2021-02-16&report_type=daily
