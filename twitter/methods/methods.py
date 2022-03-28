"""Twitter Methods Class."""
# %% codecell
import time
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate, write_to_parquet
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.twitter.user_tweets.part1_get_process import TwitterUserTweets
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.twitter.user_tweets.part3_trade_df import CreateTradeRef, CreateTradeDfV2
except ModuleNotFoundError:
    from multiuse.help_class import getDate, write_to_parquet
    from multiuse.class_methods import ClsHelp
    from twitter.methods.helpers import TwitterHelpers
    from twitter.user_tweets.part1_get_process import TwitterUserTweets
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from twitter.user_tweets.part3_trade_df import CreateTradeRef, CreateTradeDfV2

# %% codecell


class TwitterMethods(ClsHelp):
    """Handling data from twitter API."""
    # Assumed get response object with status_code == 200

    def __init__(self, rep, method, user_id, **kwargs):
        if not method:
            method = self._determine_method(self, rep)

        self.get_max_hist = kwargs.get('get_max_hist', False)
        self.fpath = self._get_fpath(self, method, user_id)
        self.df = self._call_matching_func(self, rep, method, user_id, **kwargs)

    @classmethod
    def _get_fpath(cls, self, method, user_id):
        """Get and return fpath."""
        if method == 'get_max_hist':
            return TwitterHelpers.tf('user_tweets', user_id=user_id)
        else:
            return TwitterHelpers.tf(method, user_id=user_id)

    @classmethod
    def _determine_method(cls, self, rep):
        """Determine params if method not supplied."""
        udict = ({'user_ref': 'users/by/username',
                  'user_tweets': '/tweets'})

        for key, val in udict.items():
            if val in rep.url:
                return key

    @classmethod
    def _call_matching_func(cls, self, rep, method, user_id, **kwargs):
        """Call matching function for storing data."""
        mdict = ({'user_ref': self._user_lookup,
                  # 'user_tweets': [TwitterUserTweets, TwitterUserExtract, CreateTradeRef],
                  'user_tweets': [TwitterUserTweets, TwitterUserExtract, CreateTradeDfV2],
                  'get_max_hist': TwitterUserTweets,
                  'tweet_by_id': self._tweet_by_id
                  })

        if method in ('user_ref', 'tweet_by_id'):
            return mdict[method](self, rep, method, self.fpath, user_id)
        elif self.get_max_hist:
            tut = TwitterUserTweets(rep, method, self.fpath, user_id, **kwargs)
            return tut.df
        elif method == 'user_tweets':
            df1 = mdict[method][0](rep, method, self.fpath, user_id, **kwargs)
            tue = mdict[method][1](user_id, **kwargs)
            # Create trade ref df
            # kwargs = {'df': tue.df_view}
            mdict[method][2](user_id, **kwargs)
            df, df_view = tue.df, tue.df_view
            return df_view

    @classmethod
    def _user_lookup(cls, self, rep, method, fpath, user_id):
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

    @classmethod
    def _tweet_by_id(cls, self, rep, method, fpath, user_id):
        """Store tweets + metadata in local file."""
        try:
            df = pd.json_normalize(rep.json()['data'])
            df['created_at'] = pd.to_datetime(df['created_at'])
            # For now just drop the entities column
            df.reset_index(drop=True, inplace=True)
            df.drop(columns=['entities'], inplace=True, errors='ignore')
            # Combine, but drop duplicate tweets by id
            kwargs = {'cols_to_drop': ['id']}
            write_to_parquet(df, fpath, combine=True, **kwargs)
        except KeyError as ke:
            self.elog(self, ke)
            pass

# %% codecell
