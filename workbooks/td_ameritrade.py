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
