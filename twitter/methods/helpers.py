"""Twitter helper functions."""
# %% codecell
from pathlib import Path
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

# %% codecell


class TwitterHelpers():
    """Helpers functions for twitter."""

    @staticmethod
    def twitter_fpaths(method=None, user_id=None):
        """Get matching fpath."""
        if not method:  # Default to user_ref is method not specified
            method = 'user_ref'
        bpath = Path(baseDir().path, 'social', 'twitter')
        pdict = ({'user_ref': bpath.joinpath('users', 'user_ref.parquet'),
                  'user_tweets': bpath.joinpath('users', str(user_id), '_hist_tweets.parquet')
                  })

        return pdict[method]

    @staticmethod
    def twitter_lookup_id(username):
        """Lookup twitter id based on username."""
        df_users = pd.read_parquet(TwitterHelpers.twitter_fpaths())
        user_row = df_users[df_users['username'] == username]

        if not user_row.empty:
            return user_row['id'].iloc[0]

        """
        else:
            kwargs = {'username': username}
            TwitterAPI(method='user_ref', **kwargs)
            return twitter_lookup_id(username)
        """


# %% codecell
