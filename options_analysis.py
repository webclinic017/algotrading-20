"""
Working with options data.
Classes come from options.py

Recorded dates are wrong. They should follow the last_updated.
Today is the 16th. Data is coming from the 11th still.


"""
# %% codecell
############################################################
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

import datetime
from datetime import date, timedelta

# from options import DerivativeExpirations
from all_tickers import read_symbols

# Derivatives data
from options import DerivativesHelper, DerivativesStats
from file_storage import fileOps, blockPrinting
from economic_data import read_tdata

importlib.reload(sys.modules['options'])
importlib.reload(sys.modules['file_storage'])
importlib.reload(sys.modules['economic_data'])

from options import DerivativesHelper, DerivativesStats
from file_storage import fileOps, blockPrinting
from economic_data import read_tdata

# Display max 50 columns
pd.set_option('display.max_columns', None)
# Display maximum rows
pd.set_option('display.max_rows', None)

# %% codecell
############################################################

# %% codecell
############################################################

tech_stocks = read_symbols('technology')
us_tech_stocks = tech_stocks[tech_stocks['primaryExchange'].isin(
    ['NASDAQ/NGS (GLOBAL SELECT MARKET)', 'NEW YORK STOCK EXCHANGE, INC.',
     'NASDAQ CAPITAL MARKET', 'New York Stock Exchange'])]

# %% codecell
############################################################

# Load environment variables
load_dotenv()
base_url = os.environ.get("base_url")

symbol_list = ['ALSK']

#for st in us_tech_stocks['symbol'].head(150).values:
for st in symbol_list:

    sym_data = pd.DataFrame()  # Create an empty byte array
    # print(st)

    # Load base_directory (derivatives data)
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/EOD_prices"
    # Get a list of all files in the base directory
    choices = glob.glob(f"{base_dir}/*/*")

    # Calculate the local file directory name
    fname = f"{base_dir}/{st[0].lower()}/_{st}_{DerivativesHelper.which_fname_date()}"

    # See if data is already in local directory
    if fname in choices:
        print()
        print(f"{st} data is already in the local directory. Proceeding to next ticker")
        continue

    # Get all available expirations data
    #######################################################
    payload = {'token': os.environ.get("iex_publish_api")}
    exp = requests.get(
        f"{base_url}/stock/{st}/options",
        params=payload
        )

    # Decode expiration dates from bytes to json
    try:
        exp = json.load(StringIO(exp.content.decode('utf-8')))
    except JSONDecodeError:
        print()
        print(f"Options data for {st} not found. Proceeding to next ticker")
        continue

    for dt in exp:  # For exp date in possible dates
        payload = {'token': os.environ.get("iex_publish_api")}
        rep = requests.get(
            f"{base_url}/stock/{st}/options/{dt}",
            params=payload
            )
        # Add byte array to existing byte array data
        sym_data = pd.concat([sym_data, pd.json_normalize(json.loads(rep.content))])

    # Reset index and drop
    sym_data.reset_index(drop=True, inplace=True)

    # Write pandas dataframe to local json file
    sym_data.to_json(fname)

    # Finish!
    print()
    print(f"Data recorded for {st}")


# Friday is 4
# Sunday is 6
# Monday is 0

# %% codecell
###############################################################

alsk_df = pd.read_json("/Users/unknown1/Algo/data/derivatives/EOD_prices/a/_ALSK_2021-02-15")
alsk_df.T


# %% codecell
###############################################################

    #with open(fname, 'w') as json_file:
    #    json.dump(sym_data, json_file)
    # Path(f"{base_dir}/{st[0].lower()}/_{st}_{date.today()}").write_bytes(sym_data)

# %% codecell
############################################################

# %% codecell
############################################################

recent_der_df = DerivativesHelper.read_der_data()


# %% codecell
############################################################
# %% codecell
############################################################



# %% codecell
############################################################

recent_der_df.shape

# %% codecell
##########################################################
recent_der_df.columns


# options_subset = test_pd_data[['expirationDate', 'strikePrice', 'volume', 'openInterest', 'id', 'side']].copy(deep=True)

columns_to_drop = ['cfiCode', 'currency', 'exchangeMIC', 'exchangeCode', 'closingPrice', 'marginPrice', 'isAdjusted', 'settlementPrice']
recent_der_df.drop(labels=columns_to_drop, inplace=True, axis=1)

recent_der_df.columns

def spread(df):
    """Calculate bid/ask spread."""
    df['spread'] = round(df['ask'] - df['bid'], 2)
    return df

def volume_oi(df):
    """Calulate the volume over open interest."""
    # Volume over open interest
    df['v/oi'] = round((df['volume'] / df['openInterest'] * 100), 2)
    return df

def percent_change(df, y1, y2, new_col):
    """Calculate the daily percent change."""
    # df = dataframe, y1 = "open", y2 = "close", new_col = new column name
    df[new_col] = np.where(
        df[y2] > df[y1],  # Where the price increases
        round(((df[y2] - df[y1]) / df[y1]) * 100, 1),
        round(-(1-(df[y2] / df[y1])) * 100, 1)

    )
    return df


def unusual_volume(df):
    """Look for unusual volume."""
    columns_to_use = ['symbol', 'spread', 'open', 'close', 'pChange', 'volume', 'openInterest', 'v/oi', 'contractDescription', 'id', 'side']
    # print(df[(df['v/oi'].isin(range(20, 1000000))) & (df['volume'].isin(range(40, 500000)))][columns_to_use].to_markdown())
    look_closer = df[(df['v/oi'] > 25) & (df['volume'] > 50)][columns_to_use]
    print(look_closer.to_markdown(index=False))
    return look_closer


recent_der_df = volume_oi(recent_der_df)
recent_der_df = spread(recent_der_df)
recent_der_df = percent_change(recent_der_df, 'open', 'close', 'pChange')
recent_der_df = DerivativesStats.put_call_ratio(recent_der_df)
look_closer = unusual_volume(recent_der_df)

unusual_ids = look_closer['id'].value_counts().index.values
unusual_expir = recent_der_df[recent_der_df['id'].isin(unusual_ids)].sort_values(by=['symbol', 'expirationDate', 'side'])

# Get stock dataframe and then most_recent stock dataframe
df_stocks, mr_stocks = DerivativesHelper.get_stock_data(look_closer)

unusual_expir = pd.merge(unusual_expir, mr_stocks[['symbol', 'fClose']], on='symbol', how='left')

def reorder_unusual_cols(df):
    """Reorder columns of unusual options."""
    cols_to_drop = ['contractName', 'contractSize', 'exerciseStyle', 'figi', 'lastUpdated', 'lastTradeTime', 'key', 'subkey', 'updated', 'type', 'lastTradeDate']
    df.drop(columns=cols_to_drop, axis=1, inplace=True)

    # Reindex according to expiration-date - symbol - side - strike_price
    col_list = df.columns.to_list()

    # Define order of first colunmns
    col_to_sort = ['expirationDate', 'symbol', 'strikePrice', 'side', 'fClose', 'volume', 'openInterest', 'v/oi', 'p/c']

    # Remove items in col_to_sort from column list
    [col_list.remove(col) for col in col_to_sort if col in col_list]
    # Add columns to sort to front of list
    col_to_sort.extend(col_list)
    # Reindex dataframe according to column order
    df = df.reindex(columns=col_to_sort)
    return df

unusual_expir = reorder_unusual_cols(unusual_expir)

unusual_expir.head(100)




unusual_expir.columns

df_stocks.columns

unusual_expir = percent_change(unusual_expir, 'fClose', 'strikePrice', 'sug_change')
unusual_expir['outPrem'] = (unusual_expir['open'] * unusual_expir['openInterest'] * 100).astype(int)
unusual_expir['tradedToday'] = (unusual_expir['close'] * unusual_expir['volume'] * 100).astype(int)

unusual_expir['daysToExpire'] = pd.to_datetime(unusual_expir['expirationDate'], format='%Y%m%d')
unusual_expir['daysToExpire'] = (unusual_expir['daysToExpire'] - pd.Timestamp.now()).dt.days



unusual_expir = unusual_expir[unusual_expir['tradedToday'] > 50000]
unusual_pred = unusual_expir[abs(unusual_expir['sug_change']) > 15].copy(deep=True)

base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/unusual"
unusual_fname = f"{base_dir}/unusual_{DerivativesHelper.which_fname_date()}.json"
unusual_expir.to_json(unusual_fname)


unusual_expir.columns



# Spread percentage of the close. Measure of volatitlity/no liquidity
unusual_expir['spd/%c'] = round(((unusual_expir['close'] - unusual_expir['spread']) / unusual_expir['close']) * 100, 1)
# Not that helpful from what I can see
# unusual_expir['spd/%c'].value_counts().sort_index(ascending=True)
# unusual_pred['sug_change'].sort_values(ascending=True)

unusual_pred[unusual_pred['side'].isin(['call'])].nlargest(10, 'sug_change').sort_values(by=['expirationDate', 'symbol'])
unusual_pred[unusual_pred['side'].isin(['put'])].nsmallest(10, 'sug_change').sort_values(by=['expirationDate', 'symbol'])


unusual_expir
unusual_pred

unusual_pred.groupby(by=['symbol', 'strikePrice', 'side']).sum()

fileOps.read_from_json(['RIOT'], 6)

# options_subset[options_subset['volume'].isin(range(50, 50000))]

# options_subset.groupby(by=['expirationDate', 'side']).sum().drop(labels='strikePrice', axis=1)

# Unusual volume for strike prices of the same side on different expirations

unusual_pred.shape

# %% codecell
##############################################################

import numpy as np
from scipy.stats import norm
from scipy.special import ndtr

"""
S = Spot price of the asset at time t
T = Maturity of the option
K = Strike price of the option
r = risk free interest rate
sigma = volatility of the underlying asset
"""

tdata_df = read_tdata()

tdata_df.head(10)

tdata_df['value'].max()

# %% codecell

risk_free_avg = round(tdata_df[
            pd.to_datetime(tdata_df['date']).dt.date >
            (date.today() - timedelta(days=365))
            ].head(30)['value'].mean(), 2
        )
risk_free_avg


# %% codecell
#############################################################################
N = ndtr
N_ = norm._pdf

def bs_call(S, K, T, r, vol):
    d1 = (np.log(S/K) + (r + 0.5*vol**2)*T) / (vol*np.sqrt(T))
    d2 = d1 - vol * np.sqrt(T)
    return S * N(d1) - np.exp(-r * T) * K * N(d2)

def bs_put(S, K, T, r, vol):
    d1 = (np.log(S/K) + (r + 0.5*vol**2)*T) / (vol*np.sqrt(T))
    d2 = d1 - vol * np.sqrt(T)
    return K * np.exp(-r * T) * N(-d2) - S * N(-d1)

def bs_vega(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    return S * N_(d1) * np.sqrt(T)

def find_vol_(target_value, S, K, T, r, *args):
    MAX_ITERATIONS = 200
    PRECISION = 1.0e-5
    sigma = np.full((unusual_pred['bs_price'].shape[0]), np.half(0.5))
    for se, s in enumerate(sigma):
        for i in range(0, MAX_ITERATIONS):
            price = bs_call(S.iloc[se], K.iloc[se], T.iloc[se], r, sigma[se])
            # price = bs_call(S, K, T, r, sigma)
            vega = bs_vega(S.iloc[se], K.iloc[se], T.iloc[se], r, sigma[se])
            # vega = bs_vega(S, K, T, r, sigma)
            # diff = target_value.iloc[se] - price.iloc[se]  # our root
            diff = target_value[se] - price
            if abs(diff) < PRECISION:
                break
            sigma[se] = sigma[se] + diff/vega.iloc[se]
            print(sigma[se])

        # if (abs(diff) < PRECISION):
        #     return sigma
        #  = sigma + diff/vega  # f(x) / f'(x)
    print(price)
    print(vega)
    print(diff)
    return sigma  # value wasn't found, return best guess so far

def find_vol(target_value, S, K, T, r, *args):
    MAX_ITERATIONS = 100
    ACCURACY = 0.05
    low_vol = 0
    high_vol = 1
    sigma = 0.5
    price = bs_call(S, K, T, r, sigma)
    for i in range(MAX_ITERATIONS):
        # vega = bs_vega(S, K, T, r, sigma)
        if price > target_value + ACCURACY:
            high_vol = sigma
        elif price < target_value + ACCURACY:
            low_vol = sigma
        else:
            break
        sigma = low_vol + (high_vol - low_vol)/2.0
        price = bs_call(S, K, T, r, sigma)
        # if (abs(diff) < PRECISION):
        #     return sigma
        #  = sigma + diff/vega  # f(x) / f'(x)
    return sigma  # value wasn't found, return best guess so far

# r = decimal

# %% codecell
#####################################################################
S = 49
K = 50
T = .25
# r = risk_free_avg * 4
r = .0005
vol = 0.20

V_market = bs_call(S, K, T, r, vol)
implied_vol = find_vol(V_market, S, K, T, r)

print(V_market)
print(implied_vol)
# %% codecell
#####################################################################
unusual_pred.head(10)

unusual_pred.dtypes
sigma = .5

unusual_pred_call = unusual_pred[unusual_pred['side'] == 'call'].copy(deep=True)
unusual_pred_put = unusual_pred[unusual_pred['side'] == 'put'].copy(deep=True)

# Price of the underlying asset at time t
S = unusual_pred_call['fClose']
# Strike/exercise price
K = unusual_pred_call['strikePrice']
# Time in years
T = unusual_pred_call['daysToExpire'] / 365
# Risk free interest rate
r = ((risk_free_avg/100) * 4) * T
vol = .05

r.iloc[0]

# sigma = np.full((unusual_pred['bs_price'].shape[0]), np.half(0.5))

unusual_pred_call['bs_price'] = bs_call(S, K, T, r, vol)
unusual_pred_call['bs_vol'] = np.zeros(unusual_pred_call.shape[0])

bs_vol = []

for n in range(unusual_pred_call['bs_price'].shape[0]):
    new = find_vol(unusual_pred_call['bs_price'].iloc[n], S.iloc[n], K.iloc[n], T.iloc[n], r.iloc[n])
    bs_vol.append(new)

unusual_pred_call['bs_vol'] = bs_vol
bs_vol


unusual_pred_call['bs_price']

vols = np.random.randint(15, 50, size) / 100
prices = bs_call(S, K, T, R, vols)

params = np.vstack((prices, S, K, T, R, vols))
vols = list(map(find_vol, *params))

unusual_pred['days_to_expire'] =

datetime.datetime.strptime(unusual_pred['expirationDate'].head(1), "%Y%m%d")

DerivativesHelper.which_fname_date()


# %% codecell
##############################################################

# Unusual volume for all third fridays

# third_fridays_20y = DerivativesHelper.get_all_third_fridays()
third_fridays_20y

# Only get option data for the monthly expirations to calulate mean volume
monthlies = options_subset[options_subset['expirationDate'].isin(third_fridays_20y)].groupby(by=['expirationDate', 'side']).sum()
# Calculate mean volume for monthlies
monthlies['meanVolume'] = monthlies['volume'].mean()

monthlies.shape

monthlies


# %% codecell
#############################################################


# %% codecell
#############################################################################


# %% codecell
#############################################################################

type(options_subset)
type(p_c_group)

p_c_group.columns


p_c_group.head(50)


options_subset.head(10)



options_subset[(options_subset['strikePrice'].isin([26.0]))]

options_subset['p/c'].value_counts()


p_c_group


test_der_data = json.load(test_der_data)
json.load(test_der_data)
# %% codecell
##########################################################

test_path = '/Users/unknown1/Algo/data/derivatives/EOD_prices/f/_FEYE_2021-02-11'

with open(test_path) as json_file:
    # test_der_data = json.load(json_file)

for exp in test_der_data:
    print(test_der_data[str(exp)])
    print(len(test_der_data[str(exp)]))
# json.loads(Path(test_path).read_bytes().decode("utf-8")[1:-1])
pd.json_normalize(test_der_data, record_prefix='2021').T


tech_stocks['primaryExchange'].value_counts()
tech_stocks.head(10)

# %% codecell
##########################################################
