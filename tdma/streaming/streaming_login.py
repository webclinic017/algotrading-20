"""Steaming Login request dict."""
# %% codecell
from datetime import datetime
import logging

import pandas as pd
from urllib import parse

try:
    from scripts.dev.tdma.tdma_api.td_api import TD_API
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from tdma.tdma_api.td_api import TD_API
    from multiuse.help_class import help_print_arg


# %% codecell


class TdmaStreamingLoginParams():
    """Construct request params dict for streaming."""
    # tslp : Tdma Streaming Logins

    def __init__(self, **kwargs):
        self._tslp_get_class_vars(self, **kwargs)
        # Get dataframe principals
        self.df_gpf = self._tslp_get_principals(self)
        # Create base request dictionary
        self.rbase = self._tslp_base_request_dict(self, self.df_gpf)
        # Create credential dict
        self.creds_dict = self._tslp_credential_dict(self, self.df_gpf)
        # Build request dict
        self.request_login = self._tslp_request_login(self, self.rbase)

    @classmethod
    def _tslp_get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')
        self.uri = kwargs.get('uri', 'wss://streamer-ws.tdameritrade.com/ws')
        self.requestid = 0

    @classmethod
    def _tslp_get_principals(cls, self):
        """Get principals dataframe for streaming auth call."""
        df_gp = TD_API(api_val='get_principals').df
        # Convert nested json dictionary to dataframe
        df_accounts = pd.json_normalize(df_gp['accounts'][0])
        df_gpf = df_gp.join(df_accounts).T
        df_gpf.loc['authorized'] = 'Y'

        # Test socket url against constructed url
        socket_url = df_gpf.loc['streamerInfo.streamerSocketUrl'][0]
        uri = f"wss://{socket_url}/ws"
        if uri != self.uri:  # If default uri != pulled uri
            help_print_arg("TdmaSteaming: URI does not match df_principals")

        return df_gpf

    @classmethod
    def _tslp_base_request_dict(cls, self, df_gpf):
        """Construct base dictionary. Return base dict."""
        # Where df_gpf is the get_principals dataframe
        rbase = ({
           'service': 'ADMIN',
           "command": "LOGIN",
           'requestid': self.requestid,
           'account': df_gpf.loc['accountId'][0],
           'source': df_gpf.loc['streamerInfo.appId'][0],
           'parameters': {
           }
        })

        return rbase

    @classmethod
    def _tslp_credential_dict(cls, self, df_gpf):
        """Create credential dictionary for request."""
        credential_dict = ({'company': 'company',
                            'segment': 'segment',
                            'accountCdDomainId': 'cddomain',
                            'streamerInfo.token': 'token',
                            'streamerInfo.userGroup': 'usergroup',
                            'streamerInfo.accessLevel': 'accesslevel',
                            'authorized': 'authorized',
                            'streamerInfo.tokenTimestamp': 'timestamp',
                            'streamerInfo.appId': 'appid',
                            'streamerInfo.acl': 'acl'})
        # Iterate through df principals and assign values
        creds_dict = {v: df_gpf.loc[k][0] for k, v in credential_dict.items()}
        creds_dict['userid'] = df_gpf.loc['accounts'][0][0]['accountId']
        # Convert string -> timestamp, remove microseconds
        ts = (datetime.strptime(creds_dict['timestamp'],
                                "%Y-%m-%dT%H:%M:%S%z")
                      .replace(microsecond=0))
        # Assign timestamp and convert to milliseconds
        creds_dict['timestamp'] = int(ts.timestamp()) * 1000

        return creds_dict

    @classmethod
    def _tslp_request_login(cls, self, rbase):
        """Create login request dict from request base dictionary."""
        cred_encoded = parse.quote(parse.urlencode(self.creds_dict))
        # Update rbase parameters
        rbase['parameters'] = ({
            "credential": cred_encoded,
            "token": self.df_gpf.loc['streamerInfo.token'][0],
            "version": "1.0"
        })
        # Return request dictionary
        return rbase
