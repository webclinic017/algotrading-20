"""Twitter helper functions."""
# %% codecell
from pathlib import Path
import pandas as pd
import re

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet
    from scripts.dev.multiuse.df_helpers import DfHelpers
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_parquet
    from multiuse.df_helpers import DfHelpers

# %% codecell


class TwitterHelpers():
    """Helpers functions for twitter."""

    @staticmethod
    def tf(*args, **kwargs):
        """Shorter version of calling twitter_fpaths below."""
        return TwitterHelpers.twitter_fpaths(*args, **kwargs)

    @staticmethod
    def twitter_fpaths(method=None, user_id=None, return_df=False, *args, **kwargs):
        """Get matching fpath."""
        if not method:  # Default to user_ref is method not specified
            method = 'user_ref'
        if method == 'tweets_by_id' and not user_id:
            print('TwitterHelpers.twitter_fpaths: need to pass in user_id')
        if not user_id and 'username' in kwargs.keys():
            username = kwargs.get('username', None)
            user_id = TwitterHelpers.twitter_lookup_id(username)

        bpath = Path(baseDir().path, 'social', 'twitter')
        pdict = ({'user_ref': bpath.joinpath('users', 'user_ref.parquet'),
                  'user_tweets': bpath.joinpath('users', str(user_id), '_hist_tweets.parquet'),
                  'tweet_by_id': bpath.joinpath('users', str(user_id), '_tweet_ref.parquet'),
                  'user_trades': bpath.joinpath('users', str(user_id), '_trades.parquet'),
                  'user_dir': bpath.joinpath('users', str(user_id)),
                  'all_hist': bpath.joinpath('tweets', '_hist_tweets_all.parquet'),
                  'all_trades': bpath.joinpath('tweets', '_trade_tweets_all.parquet'),
                  'all_trade_entries': bpath.joinpath('tweets', '_trades_all.parquet')
                  })

        if return_df and pdict[method].exists():
            return pd.read_parquet(pdict[method])
        else:
            return pdict[method]

    @staticmethod
    def twitter_lookup_id(username):
        """Lookup twitter id based on username."""
        f_user_ref = TwitterHelpers.twitter_fpaths('user_ref')
        if f_user_ref.exists():
            df_users = pd.read_parquet(TwitterHelpers.twitter_fpaths())
            user_row = df_users[df_users['username'] == username]

            if not user_row.empty:
                return user_row['id'].iloc[0]
        else:
            return None


        """
        else:
            kwargs = {'username': username}
            TwitterAPI(method='user_ref', **kwargs)
            return twitter_lookup_id(username)
        """

    @staticmethod
    def add_author_id_to_hist_ref():
        """Add author_id column to historical tweets dataframe."""
        bpath = Path(baseDir().path, 'social', 'twitter', 'users')
        fpaths = list(bpath.glob('**/*.parquet'))
        paths = [f for f in fpaths if '_hist_tweets' in f.name]
        tdict = {path.parent.name: path for path in paths}
        for user_id, fpath in tdict.items():
            df = pd.read_parquet(fpath).copy()
            if 'author_id' not in df.columns:
                df['author_id'] = user_id
                write_to_parquet(df, fpath)

    @staticmethod
    def combine_twitter_all(**kwargs):
        """Combine all historical tweets."""
        trade_signal = kwargs.get('trade_signal', False)
        tgrams = kwargs.get('tgrams', False)
        trades = kwargs.get('trades', False)
        return_df = kwargs.get('return_df', False)

        fpath_uref = TwitterHelpers.twitter_fpaths('user_ref')
        fdir_users = fpath_uref.parent
        fpaths_str = list(fdir_users.rglob('*.parquet'))
        paths_to_concat = []

        pattern = '_hist_tweets.parquet'
        fname = '_hist_tweets_all.parquet'
        if trade_signal:
            pattern = '_tweet_ref.parquet'
            fname = '_trade_tweets_all.parquet'
        elif tgrams:
            pattern = '_telegram_msgs.parquet'
            fname = '_telegram_msgs_all.parquet'
        elif trades:
            pattern = '_trades.parquet'
            fname = '_trades_all.parquet'

        for strpath in fpaths_str:
            if re.search(pattern, str(strpath)):
                paths_to_concat.append(strpath)

        # cols = ['id', 'author_id']
        if len(paths_to_concat) > 0:
            dfs = (pd.concat(
                [pd.read_parquet(path) for path in paths_to_concat]
                ))

            dfs = DfHelpers.combine_duplicate_columns(dfs)

            if 'id' in dfs.columns:
                dfs.drop_duplicates(subset='id', inplace=True)

            fpath_all = fdir_users.parent.joinpath('tweets', fname)
            write_to_parquet(dfs, fpath_all)

            if return_df:
                return dfs


# %% codecell
