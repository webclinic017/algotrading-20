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


class TD_API(ClsHelp, TDMA_Param_Dicts):
    """TD Ameritrade API."""
    # Only params passed through kwargs: {'params': ''}
    # will show up in the query string

    # td_bdict = TDMA_Param_Dicts()
    td_auth_class = TD_Auth

    def __init__(self, api_val, fpath=None, **kwargs):
        TDMA_Param_Dicts.__init__(self, **kwargs)
        self.verbose = kwargs.get('verbose', False)
        only_prepare_req = kwargs.get('only_prepare_req', False)
        self._get_td_auth_class(self)
        self._get_params_for_api_call(self, api_val, **kwargs)
        if not only_prepare_req:  # Check if just to prepare the request
            self.resp = self._call_api(self)
            self.df = self._call_tdma_methods(self, self.resp, api_val, **kwargs)

    @classmethod
    def _get_td_auth_class(cls, self):
        """Get TD Auth codes for API."""
        td_auth = self.td_auth_class()
        self.headers = {'Authorization': f"Bearer {td_auth.access_token}"}

    @classmethod
    def _get_params_for_api_call(cls, self, api_val, **kwargs):
        """Get params from dicts for api call."""
        self.url = self.tdma_api_endpoints.get(api_val, None)
        # Get the request type used from default dict
        self.req = self.req_dict[api_val]
        # Unpack default parameters for call
        self.payload = self.td_params.get(api_val, {})
        # Method that cleans/writes the data. tdma_params
        self.clean_func = self.td_clean_dict.get(api_val, None)
        # Unpack params in kwargs for api call payload
        params = kwargs.get('params', {})
        for k in params:
            self.payload[k] = params[k]
        body = kwargs.get('body', False)
        if body:
            self.data = body
        else:
            self.data = {}

    @classmethod
    def _call_api(cls, self):
        """Call api and convert data into dataframe, store."""
        resp = (self.req(self.url,
                         params=self.payload,
                         headers=self.headers,
                         json=self.data))
        # If the request succeeded
        if resp.status_code < 400:  # Record api call to local file tdma
            RecordAPICalls(resp, 'tdma')
        else:  # Record api call to local file tdma
            RecordAPICalls(resp, 'tdma', **{'verbose': True})
        # Return the response object
        return resp

    @classmethod
    def _call_tdma_methods(cls, self, resp, api_val, **kwargs):
        """Call TDMA method functions to clean/write data."""
        result = None
        try:
            if api_val == 'options_chain':
                gjson = resp.json()
                sym = gjson['symbol']
                result = self.clean_func(gjson, sym).df
            elif api_val == 'get_accounts':
                # self.TdmaDefault.__init__(self, api_val, resp, **kwargs)
                self.clean_func.__init__(self, api_val, resp, **kwargs)
                result = self.df
                # result = self.clean_func(gjson, sym)
                # Add df_pos (dataframe of positions) -
                # self.df_pos = result.df_pos
                # result = result.df
            elif api_val in self.req_keys:
                help_print_arg(f"{api_val} {str(resp.status_code)}")
            else:
                result = self.clean_func(api_val, resp).df
        except Exception as e:
            self.elog(self, e)

        return result

# %% codecell
