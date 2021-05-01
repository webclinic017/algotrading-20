"""Workbook for polygon.io."""


# %% codecell
#################################################

from dotenv import load_dotenv
import os
import requests
import pandas as pd
# %% codecell
#################################################


load_dotenv()
polygon_api = os.environ.get("polygon_api")
polygon_url = os.environ.get("polygon_url")
payload = {'apiKey': polygon_api, 'type': 'WARRANT', 'limit': 1000}
url = f"https://api.polygon.io/vX/reference/tickers"

get_tickers = requests.get(url, params=payload)
tickers_df = pd.DataFrame(get_tickers.json()['results'])

view_url = "https://api.polygon.io/vX/reference/tickers/VIEWW"
payload_slim = {'apiKey': polygon_api}
view_get = requests.get(view_url, params=payload_slim)
len(view_get.content)
view_get.content

view_get.json().keys()

get = requests.get(url, params=payload)
get.json()
