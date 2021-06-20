"""
Playing around with TD Ameritrades API
"""
# %% codecell
####################################
import tdameritrade as td
from tdameritrade import TDClient

from dotenv import load_dotenv
import os

# %% codecell
####################################


load_dotenv()

td_client = os.environ.get("td_client_id")
td_account = os.environ.get("td_account_id")
td_refresh = os.environ.get("td_refresh_token")

# %% codecell
####################################

tdclient = td.TDClient(client_id=td_client, refresh_token=td_refresh, account_ids=[td_account])
c = tdclient
# %% codecell
####################################

df_endp = c.optionsDF('ENDP')



td.auth.authentication

td.auth.authentication(client_id=td_client, redirect_uri='http://localhost:8080')


# %% codecell
####################################

td_url_base = 'https://auth.tdameritrade.com/'

payload = ({'response_type': 'code', 'redirect_uri': 'https://localhost:8080',
            'client_id': 'W29A4AKUBVC8B06NZWN1GNZ809V9DN8E'})

from requests import Session, Request
import requests
s = Session()
p = Request('GET', td_url_base, params=payload).prepare()
log.append(p.url)
p.url
p.url + '%40AMER.OAUTHAP'


get = requests.get(td_url_base, params=payload)
get.status_code
get.content



# %% codecell
####################################


base_url = 'https://api.tdameritrade.com/v1/oauth2/token'
payload = ({'grant_type': 'authorization_code', 'client_id': 'W29A4AKUBVC8B06NZWN1GNZ809V9DN8E', 'redirect_uri': 'https://localhost:8080'})
p = Request('POST', base_url, params=payload).prepare()
p.url
prepared_url = 'https://api.tdameritrade.com/v1/oauth2/token?grant_type=authorization_code&client_id=W29A4AKUBVC8B06NZWN1GNZ809V9DN8E&redirect_uri=https%3A%2F%2Flocalhost%3A8080'
post = requests.post(prepared_url)
post.status_code
post.content
