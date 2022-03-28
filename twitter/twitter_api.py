"""Twitter API Entry Class."""
# %% codecell
from pathlib import Path
import time
import traceback
import os
import requests
import pandas as pd
from datetime import timedelta
import inspect

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg, write_to_parquet
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.twitter.methods.methods import TwitterMethods
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg, write_to_parquet
    from twitter.methods.helpers import TwitterHelpers
    from twitter.methods.methods import TwitterMethods

# %% codecell


def _get_auth_headers(r):
    """Get twitter auth headers."""
    r.headers['Authorization'] = f"Bearer {os.environ.get('twitter_bear')}"

    return r


class TwitterAPI():
    """Get tweets. Store."""

    # methods:
    # user_ref = look up the id associated with the username
    # user_tweets : look up tweets by user.
    # Implied that the user_id is known, but will check anyway

    def __init__(self, method, **kwargs):
        self._get_class_vars(self, **kwargs)
        user_id = self._username_check(self, method, **kwargs)
        params = self._check_for_params(self, method, **kwargs)
        url = self._construct_url(self, method, user_id, **kwargs)
        self.method, self.user_id = method, user_id
        self.get = self._call_twitter_api(self, url, params)
        self.df = self._call_twitter_methods(self, self.get, method, user_id, **kwargs)

    @staticmethod
    def tweet_max_hist(username):
        """Get twitter max historical tweets for a username."""
        kwargs = ({'username': username, 'get_max_hist': True,
                   'params': {'max_results': 100,
                              'exclude': 'retweets,replies'}})
        # First call gets the first round of results - includes pag token
        call = TwitterAPI(method='user_tweets', **kwargs)
        next_token = call.get.json()['meta']['next_token']

        for n in range(31):
            try:
                kwargs['params']['pagination_token'] = next_token
                call = TwitterAPI(method='user_tweets', **kwargs)
                next_token = call.get.json()['meta']['next_token']
            except Exception as e:
                msg1 = "TwitterAPI.tweet_max_hist: "
                msg2 = f"encountered error {str(e)} {type(e)}. Breaking"
                help_print_arg(f"{msg1}{msg2}")
                help_print_arg(f"{str(call.get.json()['meta'])}")
                break

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Get class variables."""
        self.bpath = Path(baseDir().path, 'social', 'twitter')
        self.burl = "https://api.twitter.com"

        self.username = kwargs.get('username', None)

        self.verbose = kwargs.get('verbose', False)
        if self.verbose:
            help_print_arg(kwargs)

    @classmethod
    def _username_check(cls, self, method, **kwargs):
        """Check if username in local file. If not, add it."""
        if method != 'user_ref' and self.username:
            username = self.username
            user_id = TwitterHelpers.twitter_lookup_id(username)
            if not user_id:
                if self.verbose:
                    help_print_arg(f"TwitterAPI._username_check - could not find user_id")
                kwargs['exclude_params'] = True
                TwitterAPI(method='user_ref', **kwargs)
                time.sleep(2)
                user_id = TwitterHelpers.twitter_lookup_id(username)

            return user_id

        elif 'author_id' in kwargs.keys():
            return kwargs['author_id']

    @classmethod
    def _check_for_params(cls, self, method, **kwargs):
        """Check for passed parameters in kwargs."""
        params = kwargs.get('params', {})
        return params

    @classmethod
    def _construct_url(cls, self, method, user_id, **kwargs):
        """Construct url to use for the get request."""
        udict = ({'user_ref': f"{self.burl}/2/users/by/username",
                  'user_tweets': f"{self.burl}/2/users/{str(user_id)}/tweets",
                  'get_max_hist': f"{self.burl}/2/users/{str(user_id)}/tweets",
                  'tweet_by_id': f"{self.burl}/2/tweets"})

        url = None
        if method == 'user_ref' and 'username' in kwargs.keys():
            username = kwargs['username']
            url = f"{udict[method]}/{username}"
        elif method == 'tweet_by_id' and 'tweet_id' in kwargs.keys():
            url = f"{udict[method]}/{str(kwargs['tweet_id'])}"
        else:
            url = udict.get(method, False)

        if not url:
            msg1 = "TwitterAPI no methods matched for _construct_url. "
            msg2 = f"method: {str(method)}"
            print(f"{msg1}{msg2}")
        else:
            return url

    @classmethod
    def _call_twitter_api(cls, self, url, params):
        """Get tweets by a specific user."""
        isp = inspect.stack()
        get = requests.get(url, auth=_get_auth_headers, params=params)

        self._record_twitter_calls(self, get, params, isp)

        if get.status_code != 200:
            help_print_arg(f"{str(get.status_code)} {str(get.text)} {get.url}")
            if get.status_code == 429:
                heads = get.raw.getheaders()
                help_print_arg('TwitterAPI: Slow down!')

        return get

    @classmethod
    def _record_twitter_calls(cls, self, get, params, isp):
        """Records twitter calls."""
        heads = get.raw.getheaders()
        rt_left = heads['x-rate-limit-remaining']
        rt_reset = (pd.to_datetime(heads['x-rate-limit-reset'], unit='s')
                    - timedelta(hours=5))

        help_print_arg(f"{rt_left} {rt_reset}", isp=isp)

        headers = {key: val for key, val in heads.items()}
        df = pd.Series(headers).to_frame().T
        df['method'] = self.method
        df['user_id'] = self.user_id
        df['status_code'] = get.status_code
        df['url'] = get.url
        df['reason'] = get.reason

        fpath = Path(baseDir().path, 'logs', 'twitter', 'api_calls.parquet')
        write_to_parquet(df, fpath, combine=True)

    @classmethod
    def _call_twitter_methods(cls, self, get, method, user_id, **kwargs):
        """Call twitter methods for handling response, get request."""
        if self.verbose:
            help_print_arg(kwargs)
        try:
            return TwitterMethods(get, method, user_id, **kwargs).df
        except Exception as e:
            msg1 = f"TwitterAPI._call_twitter_methods: {method} {type(e)} "
            msg2 = f"{str(e)} {str(traceback.format_exc())}"
            help_print_arg(f"{msg1} {msg2}")

            if method != 'user_tweets':
                help_print_arg(f"{str(get.json())}")

            f_errors = Path(baseDir().path, 'errors', 'twitter.parquet')
            df_errors = {}
            df_errors['method'] = method
            df_errors['user_id'] = user_id
            df_errors['type'] = type(e)
            df_errors['error'] = str(e)
            df_errors['traceback'] = str(traceback.format_exc())
            df_errors = pd.Series(df_errors).to_frame().T
            write_to_parquet(df_errors, f_errors, combine=True)


# %% codecell
