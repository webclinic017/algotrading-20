"""Alpaca Authorization."""
# %% codecell

import os
from dotenv import load_dotenv

# %% codecell


class ApcaAuth():
    """Auth for Alpaca requests."""

    def __init__(self, **kwargs):
        self.burl_api = 'https://api.alpaca.markets/v2'
        # Markets base_url
        self.burl_data = 'https://data.alpaca.markets/v2'
        # Get authorization headers
        self.headers = self._load_headers(self, **kwargs)

    @classmethod
    def _load_headers(cls, self, **kwargs):
        """Load headers for Alpaca."""
        load_dotenv()
        apca_client = os.environ.get("alpaca_client_id")
        apca_secret = os.environ.get("alpaca_client_secret")

        headers = ({'APCA-API-KEY-ID': apca_client,
                    'APCA-API-SECRET-KEY': apca_secret})
        return headers

# %% codecell
