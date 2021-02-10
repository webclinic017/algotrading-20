# % codecell

import os
import json

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pathlib import Path

# %% codecell
symbols = ['AAPL', 'AMZN']

load_dotenv()
base_url = os.environ.get("base_url")

for sym in symbols:
    payload = {'token': os.environ.get("iex_publish_api")}
    rep = requests.get(
        f"{base_url}/stock/{sym}/options",
        params=payload
        )
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/expirations"
    Path(f"{base_dir}/{sym[0].lower()}/_{sym}").write_bytes(rep.content)

# %% codecell
##########################################################
load_dotenv()

base_url = os.environ.get("base_url")

symbol = 'AAPL'
payload = {'token': os.environ.get("iex_publish_api")}
rep = requests.get(
    f"{base_url}/stock/{symbol}/options",
    params=payload
)

# %% codecell
##########################################################
rep.text

# %% codecell
##########################################################
symbols = ['AAPL', 'AMZN']

for sym in symbols:
    base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/expirations"
    some_data = json.loads(Path(f"{base_dir}/{sym[0].lower()}/_{sym}").read_bytes())

    for date in some_data:
        payload = {'token': os.environ.get("iex_publish_api")}
        rep = requests.get(
            f"{base_url}/stock/{sym}/options",
            params=payload
            )
        base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/EOD_prices"
        Path(f"{base_dir}/{sym[0].lower()}/_{sym}_{date.today()}").write_bytes(rep.content)


# %% codecell
##########################################################
