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
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.remote_tasks.celery.celery_methods import CeleryMethods
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_pickle, help_print_arg
    from multiuse.api_helpers import RecordAPICalls
    from multiuse.class_methods import ClsHelp
    from remote_tasks.celery.celery_methods import CeleryMethods


# %% codecell


class CeleryAPI(ClsHelp, CeleryMethods):
    """API class for celery task queues."""

    methods = ['workers']
    """
    queues = (['production_queue',
               'captain_queue',
               'trend_queue',
               'testing_queue',
               'stream_queue'])
    """

    def __init__(self, method=None, **kwargs):

        self._capi_class_vars(self, **kwargs)
        self.fpath = self._capi_get_fpath(self, method)
        self.url = self._capi_get_url(self, method)
        self.auth = self._capi_get_auth(self)
        if not self.stop_at_auth:
            self.resp = self._capi_request_data(self, method, self.url, self.auth)
            # Store dataframe under df_cel
            self.df_cel = self._capi_extract_data(self, method, self.resp)
            # Active/reserved dataframe for tasks
            self.df_ar = self._capi_df_active_reserved(self, self.df_cel)
            # Active queues for each worker
            self.df_queues = self._capi_df_queues(self, self.df_cel)
            # Call CeleryMethods to resume streaming methods
            self._capi_cmethods(self)
            # Write output to local pickle file
            self._capi_write_to_pkl(self, self.df_cel, self.fpath)

    @classmethod
    def _capi_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        # Set class base url
        self.burl = 'https://algotrading.ventures/celery/api'

        self.verbose = kwargs.get('verbose', None)
        self.stop_at_auth = kwargs.get('stop_at_auth', None)
        # Check for streaming tasks that should be running
        self.check_streaming = kwargs.get('check_streaming', None)
        # Cancel all active/reserved tasks by routing key (queue)
        self.cancel_worker_tasks = kwargs.get('cancel_worker_tasks', None)

    @classmethod
    def _capi_get_fpath(cls, self, method):
        """Get fpath from dict."""
        bpath = Path(baseDir().path, 'logs', 'celery')
        fdict = ({
            'workers': bpath.joinpath('workers.pickle')
        })

        return fdict.get(method, fdict[list(fdict.keys())[0]])

    @classmethod
    def _capi_get_url(cls, self, method):
        """Make URl."""
        udict = ({
            'workers': 'workers'
        })

        for key, val in udict.items():
            udict[key] = f"{self.burl}/{val}"

        return udict.get(method, udict[list(udict.keys())[0]])

    @classmethod
    def _capi_get_auth(cls, self):
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
    def _capi_request_data(cls, self, method, url, auth):
        """Request data from api url."""
        isp = inspect.stack()
        params = {'refresh': True}
        get = requests.get(url, auth=auth, params=params)
        RecordAPICalls(get, 'celery')

        if get.status_code != 200:
            isp = inspect.stack()
            help_print_arg("CeleryAPI request failed", isp=isp)

        return get

    @classmethod
    def _capi_extract_data(cls, self, method, resp):
        """Apply method to response object."""
        gson = resp.json()
        df_list = []
        for key in gson.keys():
            df_t = pd.json_normalize(gson[key]).copy()
            df_t.insert(0, 'worker', key)
            df_list.append(df_t)

        df_all = pd.concat(df_list).reset_index(drop=True)

        return df_all

    @classmethod
    def _capi_df_active_reserved(cls, self, df):
        """Get dataframe subset of active and reserved tasks."""
        df_active = pd.json_normalize(df['active'].explode().dropna())
        df_reserved = pd.json_normalize(df['reserved'].explode().dropna())
        df_ar = pd.concat([df_active, df_reserved]).reset_index(drop=True)

        if df_ar.empty and self.verbose:
            help_print_arg("CeleryAPI: df_ar (active/reserved) empty")

        return df_ar

    @classmethod
    def _capi_df_queues(cls, self, df):
        """Make celery api df_queues."""
        # Extended queue information from active queue json response
        df_queues = (df['worker'].to_frame()
                                 .join(
                     pd.DataFrame.from_records(
                                  df['active_queues']
                                  .apply(lambda x: x[0]))))
        return df_queues

    @classmethod
    def _capi_cmethods(cls, self, **kwargs):
        """Instantiate celery methods class."""
        CeleryMethods.__init__(self, **kwargs)

    @classmethod
    def _capi_write_to_pkl(cls, self, df, fpath):
        """Write to local pickle file."""
        if self.verbose:
            help_print_arg(f"CeleryAPI: {str(fpath)}")
        try:
            write_to_pickle(df, fpath, combine=True)
        except EOFError as eof:
            self.elog(self, eof)
            print("CeleryAPI data available under self.df_cel")


# %% codecell
