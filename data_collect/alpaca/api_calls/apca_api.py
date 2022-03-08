"""Alpaca API Base Class."""
# %% codecell

try:
    from scripts.dev.data_collect.alpaca.api_calls.apca_params import ApcaParams
    from scripts.dev.data_collect.alpaca.api_calls.apca_auth import ApcaAuth
    from scripts.dev.data_collect.alpaca.methods.apca_paths import ApcaPaths
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.multiuse.api_helpers import RecordAPICalls
except ModuleNotFoundError:
    from data_collect.alpaca.api_calls.apca_params import ApcaParams
    from data_collect.alpaca.api_calls.apca_auth import ApcaAuth
    from data_collect.alpaca.methods.apca_paths import ApcaPaths
    from multiuse.class_methods import ClsHelp
    from multiuse.api_helpers import RecordAPICalls

# %% codecell


class ApcaAPI(ApcaPaths, ApcaAuth, ApcaParams, ClsHelp):
    """Base API Caller for Alpaca."""
    verbose = True

    def __init__(self, api_val, **kwargs):
        ClsHelp.__init__(self)
        self.verbose = kwargs.get('verbose', False)
        only_prepare_req = kwargs.get('only_prepare_req', False)
        self._get_fpaths(self, api_val, **kwargs)
        self._get_auth_headers(self, **kwargs)
        self._get_params(self, **kwargs)
        self._get_params_for_api_call(self, api_val, **kwargs)
        if not only_prepare_req:  # Check if just to prepare the request
            self.resp = self._call_api(self, **kwargs)
            self.result = self._call_apca_methods(self, self.resp, api_val, **kwargs)

    @classmethod
    def _get_fpaths(cls, self, api_val, **kwargs):
        """Initialize apca fpaths."""
        ApcaPaths.__init__(self, api_val, **kwargs)

    @classmethod
    def _get_auth_headers(cls, self, **kwargs):
        """Initialize auth headers class."""
        ApcaAuth.__init__(self, **kwargs)

    @classmethod
    def _get_params(cls, self, **kwargs):
        """Initialize Apca parameter class."""
        ApcaParams.__init__(self, **kwargs)

    @classmethod
    def _get_params_for_api_call(cls, self, api_val, **kwargs):
        """Get parameters for subsequent API call."""
        # Get the url
        self.url = self.apca_endpoints.get(api_val, None)
        # Get the request type used from default dict
        self.req = self.req_dict[api_val]
        # Unpack default parameters for call
        self.payload = self.apca_params.get(api_val, {})
        # Method that cleans/writes the data. apca_params
        self.clean_func = self.apca_clean_dict.get(api_val, None)
        # Unpack params in kwargs for api call payload
        params = kwargs.get('params', {})
        for k in params:
            self.payload[k] = params[k]

        if self.verbose:
            print(str(api_val))
            print(f"url {str(self.url)}")
            print(f"payload {str(self.payload)}")

    @classmethod
    def _call_api(cls, self, **kwargs):
        """Call api and convert data into dataframe, store."""
        resp = (self.req(self.url,
                         params=self.payload,
                         headers=self.headers))
        # If the request succeeded
        if resp.status_code < 400:  # Record api call to local file tdma
            RecordAPICalls(resp, 'apca')
        else:  # Record api call to local file tdma
            RecordAPICalls(resp, 'apca', **{'verbose': True})
        # Return the response object
        return resp

    @classmethod
    def _call_apca_methods(cls, self, resp, api_val, **kwargs):
        """Call APCA method functions to clean/write data."""
        try:
            result = self.clean_func(resp, self.fpath, **kwargs)
            self.df = result.df
            self.next_page_token = result.next_page_token
            return result
        except Exception as e:
            self.elog(self, e)
            return None
