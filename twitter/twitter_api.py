"""Twitter API Entry Class."""
# %% codecell
from pathlib import Path
import time
import traceback
import os
import requests
import pandas as pd

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
    r.headers['User-Agent'] = "v2UserLookupPython"

    return r


class TwitterAPI():
    """Get tweets. Store."""

    # methods:
    # user_ref = look up the id associated with the username
    # user_tweets : look up tweets by user.
    # Implied that the user_id is known, but will check anyway

    def __init__(self, method, **kwargs):
        self._get_class_vars(self)
        user_id = self._username_check(self, method, **kwargs)
        params = self._check_for_params(self, method, **kwargs)
        url = self._construct_url(self, method, user_id, **kwargs)
        self.get = self._call_twitter_api(self, url, params)
        self.method, self.user_id = method, user_id
        self.df = self._call_twitter_methods(self, self.get, method, user_id)

    def __call__(self):
        self.df = (self._call_twitter_methods(self, self.get,
                                              self.method, self.user_id))

    @staticmethod
    def tweet_max_hist(username):
        """Get twitter max historical tweets for a username."""
        kwargs = ({'username': username,
                   'params': {'max_results': 100,
                              'exclude': 'replies'}})
        # First call gets the first round of results - includes pag token
        call = TwitterAPI(method='user_tweets', **kwargs)
        next_token = call.get.json()['meta']['next_token']

        for n in range(31):
            try:
                kwargs['params']['pagination_token'] = next_token
                call = TwitterAPI(method='user_tweets', **kwargs)
                next_token = call.get.json()['meta']['next_token']
            except KeyError:
                msg1 = "TwitterAPI.tweet_max_hist: "
                msg2 = "encountered KeyError. Breaking"
                help_print_arg(f"{msg1}{msg2}")
                help_print_arg(f"{str(call.get.json()['meta'])}")
                break

    @classmethod
    def _get_class_vars(cls, self):
        """Get class variables."""
        self.bpath = Path(baseDir().path, 'social', 'twitter')
        self.burl = "https://api.twitter.com"

    @classmethod
    def _username_check(cls, self, method, **kwargs):
        """Check if username in local file. If not, add it."""
        if method != 'user_ref' and 'username' in kwargs.keys():
            username = kwargs['username']
            user_id = TwitterHelpers.twitter_lookup_id(username)
            if not user_id:
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
        if method == 'user_tweets' and 'params' in kwargs.keys():
            return kwargs['params']
        elif method == 'tweet_by_id' and 'params' in kwargs.keys():
            return kwargs['params']
        else:
            return {}

    @classmethod
    def _construct_url(cls, self, method, user_id, **kwargs):
        """Construct url to use for the get request."""
        udict = ({'user_ref': f"{self.burl}/2/users/by/username",
                  'user_tweets': f"{self.burl}/2/users/{str(user_id)}/tweets",
                  'tweet_by_id': f"{self.burl}/2/tweets"})

        if method == 'user_ref' and 'username' in kwargs.keys():
            username = kwargs['username']
            return f"{udict[method]}/{username}"
        elif method == 'user_tweets' and user_id:
            return udict[method]
        elif method == 'tweet_by_id' and 'tweet_id' in kwargs.keys():
            return f"{udict[method]}/{str(kwargs['tweet_id'])}"
        else:
            msg1 = "TwitterAPI no methods matched for _construct_url. "
            msg2 = f"method: {str(method)}"
            print(f"{msg1}{msg2}")

    @classmethod
    def _call_twitter_api(cls, self, url, params):
        """Get tweets by a specific user."""
        get = requests.get(url, auth=_get_auth_headers, params=params)

        if get.status_code != 200:
            print(f"{str(get.status_code)} {str(get.text)} {get.url}")
            if get.status_code == 429:
                help_print_arg('TwitterAPI: Slow down!')

        return get

    @classmethod
    def _call_twitter_methods(cls, self, get, method, user_id):
        """Call twitter methods for handling response, get request."""
        try:
            return TwitterMethods(get, method, user_id).df
        except Exception as e:
            msg1 = f"TwitterAPI._call_twitter_methods: {method} {type(e)} "
            msg2 = f"{str(e)} {str(traceback.format_exc())}"
            help_print_arg(f"{msg1} {msg2}")
            help_print_arg(f"{str(get.json())}")

            f_errors = Path(baseDir().path, 'errors', 'twitter.parquet')
            df_errors = pd.DataFrame()
            df_errors['method'] = method
            df_errors['user_id'] = user_id
            df_errors['type'] = type(e)
            df_errors['error'] = str(e)
            df_errors['traceback'] = str(traceback.format_exc())
            write_to_parquet(df_errors, f_errors, combine=True)


# %% codecell
