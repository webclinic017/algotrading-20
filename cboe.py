"""
CBOE Options Data
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

from pandas_df_options import freeze_header

import xml.etree.ElementTree as ET

# %% codecell
##############################################################

base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/cboe"

cboe_base = "https://www.cboe.com/us/options/market_statistics/historical_data/download/"
cboe_u1 = "all_symbols/?reportType=volume&month=&year=2021&volumeType=sum&volumeAggType=daily"
cboe_u2 = "&exchanges=CBOE&exchanges=BATS&exchanges=C2&exchanges=EDGX"

cboe_url = f"{cboe_base}{cboe_u1}{cboe_u2}"
cboe_get = requests.get(cboe_url)

print(type(cboe_get.content))
print(len(cboe_get.content))

cboe_sample = cboe_get.content[0:10000]
cboe_sample

print(CnM.from_bytes(cboe_sample).best().first())


# %% codecell
##############################################################

cboe_df = pd.read_csv(
            BytesIO(cboe_get.content),
            escapechar='\n',
            delimiter=',',
            delim_whitespace=False,
            skipinitialspace=False
            )

cboe_df.shape


cboe_df.head(10)

# %% codecell
##############################################################
"""
Missed Liquidity
    This is a measure for the last week of the average daily volume requested at a price equal or better than the NBBO where we had no liquidity.
Exhausted Liquidity
    This is a measure for the last week of the average daily volume requested at a price equal or better than the NBBO, which were partially filled.
Routed Liquidity
    This number represents for the last week the average daily volume on orders which were routed and filled on another venue.
Volume Opportunity
    This is a measure of the total average daily volume of the missed, exhausted and routed liquidity.
Cboe ADV
    Average Daily Volume for the last week of shares matched on Cboe for the security shown.
Liquidity Opportunity
    Percentage of the ADV missed, exhausted or routed. The higher the percentage the larger the market making opportunity.
"""
mm_base_url = "https://www.cboe.com/us/options/market_statistics/maker_report/csv/?book=all&mkt="
mm_dict = { 'cone': '', 'opt': '', 'ctwo': '', 'exo': ''}

for k in mm_dict.keys():
    mm_dict[k] = requests.get(f"{mm_base_url}{k}")



https://www.cboe.com/us/options/market_statistics/maker_report/csv/?book=all&mkt=cone

https://www.cboe.com/us/options/market_statistics/maker_report/csv/?book=all&mkt=opt

https://www.cboe.com/us/options/market_statistics/maker_report/?mkt=ctwo

https://www.cboe.com/us/options/market_statistics/maker_report/csv/?book=all&mkt=exo
