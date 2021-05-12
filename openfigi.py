"""Workbook for openfigi."""
# %% codecell
##################################################
from datetime import date
import json
from io import BytesIO
import os

import requests
import pandas as pd

# Display max 50 columns
pd.set_option('display.max_columns', None)

# Display maximum rows
pd.set_option('display.max_rows', 500)

# %% codecell
##################################################

base_url = 'https://api.openfigi.com/v3/mapping'
base_url = 'https://api.openfigi.com/v3/filter'
payload = {'query': ['VIEW']}

from datetime import timedelta
years_future_6 = (date.today() + timedelta(days=(365 * 6))).strftime("%Y-%m-%d")
exp_range = [date.today().strftime("%Y-%m-%d"), years_future_6]
payload = [{"idType": "BASE_TICKER", "idValue": "VIEW", "securityType2": "Common Stock", 'exchCode': 'US'}]
payload = [{"idType": "BASE_TICKER", "idValue": "VIEW", "securityType2": "Warrant", "expiration": [date.today().strftime("%Y-%m-%d"), None], 'exchCode': 'US'}]
payload = [{"idType": "TICKER", "idValue": "VIEW",'exchCode': 'US'}]
headers = {'content-type': 'text/json'}

url_search = 'https://api.openfigi.com/v3/search'
payload_search = {'query': 'VIEWW', 'exchCode': 'US'}
search_post = requests.post(url_search, data=json.dumps(payload_search), headers=headers)
search_post.content

search_df = pd.DataFrame(search_post.json()['data'])
search_df.head(10)

search_df[search_df['ticker'] == 'VIEWW']
search_df['securityType2'].value_counts()
search_df[search_df['']]

post = requests.post(base_url, data=json.dumps(payload), headers=headers)
post.content
[post_json] = json.load(BytesIO(post.content))
post_json
post_df = pd.DataFrame(post_json['data'])
post_df
post_df.head(1)
post.content


# fpath = "/Users/unknown1/Algo/data/sec/inst_holds_com/_comb.gz"


# %% codecell
##################################################
from dotenv import load_dotenv
load_dotenv()

figi_api = os.environ.get("openfigi_api")

fpath = "/Users/unknown1/Algo/data/sec/inst_holds_com/_comb.gz"
comb_holdings_df = pd.read_json(fpath, compression='gzip')
cusip_list = comb_holdings_df['cusip'].value_counts().index.tolist()

comb_holdings_df.head(10)

headers = {'content-type': 'text/json', 'X-OPENFIGI-APIKEY': figi_api}
base_url = 'https://api.openfigi.com/v3/mapping'
url = f"{base_url}"

payload_all = [{'idType': 'ID_CUSIP', 'idValue': val} for val in cusip_list]
all_df_list = []
while len(payload_all) != 0:
    payload = []
    for n, val in enumerate(payload_all):
        if n <= 98:
            payload.append(payload_all[n])
            payload_all.remove(val)
        else:
            break

    if len(payload_all) <= 99:
        if (len(payload_all) == len(payload)):
            post = requests.post(url, data=json.dumps(payload), headers=headers)
            #df = pd.DataFrame(post.json()[0]['data']).copy(deep=True)
            #df['cusip'] = [payload[num]['idValue'] for num in range(len(payload))]
            all_df_list.append(pd.DataFrame(post.json()[0]['data']))
            break
    else:
        post = requests.post(url, data=json.dumps(payload), headers=headers)
        #df = pd.DataFrame(post.json()[0]['data']).copy(deep=True)
        #df['cusip'] = [payload[num]['idValue'] for num in range(len(payload))]
        all_df_list.append(pd.DataFrame(post.json()[0]['data']))


all_df = pd.concat(all_df_list)
all_df.shape
all_df.drop_duplicates(subset='ticker', inplace=True)

all_df['exchCode'].value_counts()

all_df['securityDescription'].value_counts()

comb_holdings_df.head(50)

all_df.head(10)

df['cusip'] = [payload[num]['idValue'] for num in range(len(payload))]
all_df['ticker']

all_df.head(10)

from api import serverAPI
all_syms = serverAPI('all_symbols').df
comb_holdings_df.rename(columns={'nameOfIssuer': 'name'}, inplace=True)

all_syms['name_low'] = all_syms['name'].str.lower()
comb_holdings_df['name_low'] = comb_holdings_df['name'].str.lower()

comb_test = pd.merge(comb_holdings_df, all_syms, on='name_low', indicator=True)
comb_test['name_low'].value_counts()
comb_test.head(50)

comb_holdings_df['name_low'].value_counts().head(50)
comb_holdings_df.head(10)

all_syms = serverAPI('all_symbols').df
all_syms.head(5)

# %% codecell
##################################################
