"""Class for Celery Tasks API."""
# %% codecell
from pathlib import Path
import inspect
import os


import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_pickle, help_print_arg
    from scripts.dev.multiuse.api_helpers import RecordAPICalls
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_pickle, help_print_arg
    from multiuse.api_helpers import RecordAPICalls


# %% codecell


class CeleryAPI():
    """API class for celery task queues."""

    methods = ['workers']

    def __init__(self, method, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.fpath = self._get_fpath(self, method)
        self.url = self._get_url(self, method)
        auth = self._get_auth(self)
        self.resp = self._request_data(self, method, self.url, auth)
        self.df = self._apply_methods(self, method, self.resp)
        self._write_to_pkl(self, self.df, self.fpath)

    @classmethod
    def _get_fpath(cls, self, method):
        """Get fpath from dict."""
        bpath = Path(baseDir().path, 'logs', 'celery')
        fdict = ({
            'workers': bpath.joinpath('workers.pickle')
        })

        return fdict[method]

    @classmethod
    def _get_url(cls, self, method):
        """Make URl."""
        base_url = 'https://algotrading.ventures/celery/api/'
        udict = ({
            'workers': 'workers'
        })

        for key, val in udict.items():
            udict[key] = f"{base_url}{val}"

        return udict[method]

    @classmethod
    def _get_auth(cls, self):
        """Get authorization param."""
        load_dotenv()
        user = os.environ.get('celery_user')
        password = os.environ.get('celery_pass')

        if user and password:
            auth = HTTPBasicAuth(user, password)
            return auth
        else:
            print('_get_auth: either user or password is null')

    @classmethod
    def _request_data(cls, self, method, url, auth):
        """Request data from api url."""
        isp = inspect.stack()
        get = requests.get(url, auth=auth)
        RecordAPICalls(get, 'celery')

        if get.status_code != 200:
            isp = inspect.stack()
            help_print_arg("CeleryAPI request failed", isp=isp)

        return get

    @classmethod
    def _apply_methods(cls, self, method, resp):
        """Apply method to response object."""
        gson = resp.json()
        df_list = []
        for key in gson.keys():
            df_t = pd.json_normalize(gson[key]).copy()
            df_t.insert(0, 'worker', key)
            df_list.append(df_t)

        df_all = pd.concat(df_list)

        return df_all

    @classmethod
    def _write_to_pkl(cls, self, df, fpath):
        """Write to local pickle file."""
        if self.verbose:
            help_print_arg(f"CeleryAPI: {str(fpath)}")

        write_to_pickle(df, fpath, combine=True)

# %% codecell
