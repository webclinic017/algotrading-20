"""
Test API endpoints for status code == 200
"""
# %% codecell
#################################################
import json

import pytest
import requests

# %% codecell
#################################################

base_url = "https://algotrading.ventures/api/v1"
url_dict = ({
    'treasuries': '/econ/treasuries',
    'cboe_mmo_raw': '/cboe/mmo/raw',
    'cboe_mmo_top': '/cboe/mmo/top',
    'cboe_mmo_syms': '/cboe/mmo/syms',
    'st_stream': '/stocktwits/user_stream',
    'st_trend': '/stocktwits/trending',
    'iex_quotes_raw': '/iex_eod_quotes',
    'new_symbols': '/symbols/new',
    'all_symbols': '/symbols/all'
})

def test_endpoints():
    """Test all endppoints."""
    for endpoint in url_dict:
        url = f"{base_url}{url_dict[endpoint]}"
        get = requests.get(url)

        assert get.status == 200
