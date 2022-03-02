"""API for TD Ameritrade."""
# %% codecell

import requests

try:
    from scripts.dev.multiuse.help_class import help_print_arg
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.multiuse.api_helpers import RecordAPICalls
    from scripts.dev.tdma.tdma_api.tdma_params import TDMA_Param_Dicts
    from scripts.dev.tdma.tdma_api.td_auth import TD_Auth
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
    from multiuse.class_methods import ClsHelp
    from multiuse.api_helpers import RecordAPICalls
    from tdma.tdma_api.tdma_params import TDMA_Param_Dicts
    from tdma.tdma_api.td_auth import TD_Auth

# %% codecell


class TD_API(ClsHelp):
    """TD Ameritrade API."""

    td_bdict = TDMA_Param_Dicts()
    td_auth_class = TD_Auth
    td_url_dict = td_bdict.tdma_api_endpoints
    td_params_dict = td_bdict.td_params
    td_clean_dict = td_bdict.td_clean_dict
    td_result = None

    def __init__(self, api_val, fpath=None, **kwargs):
        self._get_td_auth_class(self)
        self._get_params_for_api_call(self, api_val, **kwargs)
        self.get = self._call_api(self)
        self.df = self._call_tdma_methods(self, self.get, **kwargs)

    @classmethod
    def _get_td_auth_class(cls, self):
        """Get TD Auth codes for API."""
        td_auth = self.td_auth_class()
        self.headers = {'Authorization': f"Bearer {td_auth.access_token}"}

    @classmethod
    def _get_params_for_api_call(cls, self, api_val, **kwargs):
        """Get params from dicts for api call."""
        self.url = self.td_url_dict[api_val]
        self.payload = self.td_params_dict[api_val]
        # Method that cleans/writes the data. tdma_params
        self.clean_func = self.td_clean_dict[api_val]
        # Unpack params in kwargs for api call payload
        params = kwargs.get('params', {})
        for k in params:
            self.payload[k] = params[k]

    @classmethod
    def _call_api(cls, self):
        """Call api and convert data into dataframe, store."""
        get = (requests.get(self.url,
                            params=self.payload,
                            headers=self.headers))
        # If the get request succeeded
        if get.status_code < 400:  # Record api call to local file tdma
            RecordAPICalls(get, 'tdma')
        else:  # Record api call to local file tdma
            RecordAPICalls(get, 'tdma', **{'verbose': True})
        # Return get object
        return get

    @classmethod
    def _call_tdma_methods(cls, self, get, **kwargs):
        """Call TDMA method functions to clean/write data."""
        gjson = get.json()
        result = None
        try:
            sym = gjson['symbol']
            result = self.clean_func(gjson, sym).df
        except Exception as e:
            self.elog(self, e)

        return result

# %% codecell
