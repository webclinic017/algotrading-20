"""TD Ameritrade Parameters for Func Calls."""
# %% codecell
import os
from collections import defaultdict
from datetime import timedelta
from dotenv import load_dotenv

import requests

try:
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.tdma.tdma_methods.td_clean_options import TD_Clean_Write_Options
    from scripts.dev.tdma.tdma_methods.default_methods import TdmaDefault
    from scripts.dev.tdma.tdma_methods.top_movers import TdmaSectorMovers
except ModuleNotFoundError:
    from multiuse.help_class import getDate
    from tdma.tdma_methods.td_clean_options import TD_Clean_Write_Options
    from tdma.tdma_methods.default_methods import TdmaDefault
    from tdma.tdma_methods.top_movers import TdmaSectorMovers

# %% codecell


class TDMA_Param_Dicts():
    """Dictionaries of TDMA Parameters."""

    """
    param: self.dt : base date used
    param: self.tdma_api_endpoints : dict of endpoints
    param: self.td_params : commonly used params for endpoints
    param: self.td_clean_write : dict of functions to clean data
    """
    klist = (['get_transactions', 'get_accounts', 'get_principals',
              'get_orders', 'get_quotes', 'get_sub_keys'])

    def __init__(self, **kwargs):
        self._construct_class_params(self)
        self._tdma_api_endpoints(self, **kwargs)
        self._tdma_request_type(self)
        self._tdma_api_params(self)
        self._tdma_clean_write_functions(self)

    @classmethod
    def _construct_class_params(cls, self):
        """Construct params for use in other class functions."""
        self.dt = getDate.query('iex_close')

    @classmethod
    def _tdma_api_endpoints(cls, self, **kwargs):
        """Endpoints for TDMA API."""
        burl = 'https://api.tdameritrade.com/v1'
        load_dotenv()
        accountId = os.environ.get('tdma_id')
        orderId = kwargs.get('orderId', False)
        sector = kwargs.get('sector', False)

        tdma_api_endpoints = ({
            'options_chain': f'{burl}/marketdata/chains',
            'get_quotes': f"{burl}/marketdata/quotes",
            'get_accounts': f'{burl}/accounts/{accountId}',
            'get_orders': f'{burl}/accounts/{accountId}/orders',
            'get_transactions': f"{burl}/accounts/{accountId}/transactions",
            'get_principals': f"{burl}/userprincipals",
            'get_sub_keys': f"{burl}/userprincipals/streamersubscriptionkeys",
            'get_instruments': f"{burl}/instruments",
            'get_movers': f"{burl}/marketdata/{sector}/movers",
            'cancel_order': f"{burl}/accounts/{accountId}/orders/{orderId}",
            'place_order': f"{burl}/accounts/{accountId}/orders"
        })

        self.tdma_api_endpoints = tdma_api_endpoints

    @classmethod
    def _tdma_request_type(cls, self):
        """Request dict for different types."""
        tdma_request_type = ({
            'cancel_order': requests.delete,
            'cancel_all_orders': requests.delete,
            'replace_order': requests.put,
            'place_order': requests.post,
        })

        def default_val():
            return requests.get
        # Create default dictionary with requests.get as default value
        req_dict = defaultdict(default_val, tdma_request_type)
        self.req_dict = req_dict
        self.req_keys = list(req_dict.keys())

    @classmethod
    def _tdma_api_params(cls, self):
        """Add default parameters for dictionary."""
        to_dt = self.dt + timedelta(weeks=(52 * 2))

        td_params = ({
            'options_chain':
            {'symbol': '', 'includeQuotes': 'TRUE', 'toDate': str(to_dt)},
            'get_accounts':
            {'fields': 'positions,orders'},
            'get_orders':  # Only get orders for today
            {'maxResults': '10', 'fromEnteredTime': str(self.dt)},
            'get_instruments':
            {'symbol': 'OCGN', 'projection': 'symbol-search'},
            'get_principals':
            {'fields':
             'streamerSubscriptionKeys,streamerConnectionInfo'}
            # 'streamerSubscriptionKeys,streamerConnectionInfo,preferences,surrogateIds'
        })

        self.td_params = td_params

    @classmethod
    def _tdma_clean_write_functions(cls, self):
        """Dictionary of commonly used cleaning functions."""
        td_clean_dict = ({
            'options_chain': TD_Clean_Write_Options,
            'get_movers': TdmaSectorMovers,
        })
        # List comprehension for default methods
        [td_clean_dict.setdefault(key, TdmaDefault) for key in self.klist]

        self.td_clean_dict = td_clean_dict


# %% codecell
