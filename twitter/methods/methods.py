"""Twitter Methods Class."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate, write_to_parquet
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.twitter.methods.user_tweets import TwitterUserTweets
except ModuleNotFoundError:
    from multiuse.help_class import getDate, write_to_parquet
    from twitter.methods.helpers import TwitterHelpers
    from twitter.methods.user_tweets import TwitterUserTweets

# %% codecell


class TwitterMethods():
    """Handling data from twitter API."""
    # Assumed get response object with status_code == 200

    def __init__(self, rep, method, user_id):
        if not method:
            method = self._determine_method(self, rep)
        self.fpath = TwitterHelpers.twitter_fpaths(method, user_id=user_id)
        self.df = self._call_matching_func(self, rep, method)

    @classmethod
    def _determine_method(cls, self, rep):
        """Determine params if method not supplied."""
        udict = ({'user_ref': 'users/by/username',
                  'user_tweets': '/tweets'})

        for key, val in udict.items():
            if val in rep.url:
                return key

    @classmethod
    def _call_matching_func(cls, self, rep, method):
        """Call matching function for storing data."""
        mdict = ({'user_ref': self._user_lookup,
                  'user_tweets': TwitterUserTweets
                  })
        if method == 'user_ref':
            return mdict[method](self, rep, method, self.fpath)
        elif method == 'user_tweets':
            return mdict[method](rep, method, self.fpath).df

    @classmethod
    def _user_lookup(cls, self, rep, method, fpath):
        """Look up user ID based on username. Store locally."""
        data = rep.json()['data']
        # Add timestamp on results (in case user changes username)
        dt = getDate.query('iex_close')
        data['time'] = (pd.Timestamp(year=dt.year,
                        month=dt.month, day=dt.day))
        # Convert to dataframe, then combine with local file
        df = pd.DataFrame.from_dict(data, orient='index').T

        if fpath.exists():
            df_old = pd.read_parquet(fpath)
            if not df.columns.difference(list(data.keys())).empty:
                print('Columns are not equal')
            else:
                df = pd.concat([df, df_old]).copy()

        write_to_parquet(df, fpath, combine=True, drop_duplicates=True)
        return df

# %% codecell
