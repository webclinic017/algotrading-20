"""Workbook for openfigi."""

# %% codecell
##################################################



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


# %% codecell
##################################################
