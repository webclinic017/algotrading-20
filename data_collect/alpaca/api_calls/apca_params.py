"""Apca params class."""
# %% codecell
from collections import defaultdict
from datetime import timedelta

import requests
import pandas as pd

try:
    from scripts.dev.data_collect.alpaca.news.historical import ApcaNewsHistorical
    from scripts.dev.data_collect.alpaca.methods.announcements import ApcaAnnouncements
    from scripts.dev.data_collect.alpaca.methods.calendar import ApcaCalendar
except ModuleNotFoundError:
    from data_collect.alpaca.news.historical import ApcaNewsHistorical
    from data_collect.alpaca.methods.announcements import ApcaAnnouncements
    from data_collect.alpaca.methods.calendar import ApcaCalendar

# %% codecell


class ApcaParams():
    """Alpaca Parameters for API Calls."""

    burl_api = 'https://api.alpaca.markets/v2'
    burl_data = 'https://data.alpaca.markets/v2'

    def __init__(self, **kwargs):
        self._api_endpoints(self, **kwargs)
        self._apca_request_type(self, **kwargs)
        self._apca_api_params(self, **kwargs)
        self._apca_clean_write_functions(self, **kwargs)

    @classmethod
    def _api_endpoints(cls, self, **kwargs):
        """Construct dict of url_endpoints."""

        apca_endpoints = ({
            'news_historical': 'https://data.alpaca.markets/v1beta1/news',
            'announcements': f'{self.burl_api}/corporate_actions/announcements',
            'calendar': f"{self.burl_api}/calendar"
        })

        self.apca_endpoints = apca_endpoints

    @classmethod
    def _apca_request_type(cls, self, **kwargs):
        """Request dict for different types."""
        # Add methods as needed
        apca_request_type = ({})

        def default_val():
            return requests.get
        # Create default dictionary with requests.get as default value
        req_dict = defaultdict(default_val, apca_request_type)
        self.req_dict = req_dict
        self.req_keys = list(req_dict.keys())

    @classmethod
    def _apca_api_params(cls, self, **kwargs):
        """Add default parameters for dictionary."""
        until = pd.Timestamp.now().date()
        since = until - timedelta(days=89)

        apca_params = ({
            'news_historical': ({'limit': 50, 'include_content': True}),
            'announcements': ({'since': since, 'until': until})

        })

        self.apca_params = apca_params

    @classmethod
    def _apca_clean_write_functions(cls, self, **kwargs):
        """Dictionary of commonly used cleaning functions."""
        # Add methods as needed
        apca_clean_dict = ({
            'news_historical': ApcaNewsHistorical,
            'announcements': ApcaAnnouncements,
            'calendar': ApcaCalendar
        })

        def default_func():
            return None

        apca_c_default_dict = defaultdict(default_func, apca_clean_dict)
        # List comprehension for default methods
        # [td_clean_dict.setdefault(key, TdmaDefault) for key in self.klist]

        self.apca_clean_dict = apca_c_default_dict
