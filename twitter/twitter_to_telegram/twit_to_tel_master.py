"""Twitter to telegram master class."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.twitter.twitter_to_telegram.classes_twit_to_tel import (
        TwitterRecentMessagesUser, TwitterUnsentTelegramMsgs,
        MakeTradeableMessages, SendTelegramPollFromTrade)
except ModuleNotFoundError:
    from multiuse.class_methods import ClsHelp
    from twitter.methods.helpers import TwitterHelpers
    from twitter.twitter_to_telegram.classes_twit_to_tel import (
        TwitterRecentMessagesUser, TwitterUnsentTelegramMsgs,
        MakeTradeableMessages, SendTelegramPollFromTrade)

# %% codecell


class TwitterToTelegram(TwitterRecentMessagesUser, TwitterUnsentTelegramMsgs,
                        MakeTradeableMessages, SendTelegramPollFromTrade,
                        ClsHelp):
    """Top Class for Twitter to Telegram functions."""

    def __init__(self, **kwargs):
        self._get_class_vars(self, **kwargs)
        self._get_user_data(self, **kwargs)

        self._get_recent_messages(self, **kwargs)
        self._unsent_telegram_msgs(self, **kwargs)
        self._make_tradeable_messages(self, **kwargs)
        self._send_telegram_polls_from_trade(self, **kwargs)

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Unpack class variables from kwargs."""
        th = TwitterHelpers.twitter_fpaths
        # All twitter users reference dataframe
        self.df_uref = kwargs.get('df_uref', pd.read_parquet(th('user_ref')))

        self.telegram = kwargs.get('telegram', False)
        self.testing = kwargs.get('testing', False)
        self.verbose = kwargs.get('verbose', False)
        # Max number of tweets to get from twitter
        self.max_results = kwargs.get('max_results', 5)
        # Default 75 seconds in the past - pull all tweets
        self.offset = kwargs.get('offset', 75)
        # Time filter - TwitterUnsentTelegramMsgs
        self.time_filter = kwargs.get('time_filter', 60**3)
        # Testing: place each API call in self.call_list
        self.call_list = []

    @classmethod
    def _get_user_data(cls, self, **kwargs):
        """Get and store all user dfs in class dict."""
        user_dict = {}
        for index, row in self.df_uref.iterrows():
            fpath_reft = TwitterHelpers.tf('tweet_by_id', row['id'])
            user_dict[row['id']] = ({
                'username': row['username'],
                'fpath_reft': fpath_reft,
                'tweet_by_id': pd.read_parquet(fpath_reft),
                'fpath_tgrams': (TwitterHelpers.tf('user_dir', row['id'])
                                 .joinpath('_telegram_msgs.parquet'))
            })
        # Assign user_dict to class var
        self.user_dict = user_dict

    @classmethod
    def _get_recent_messages(cls, self, **kwargs):
        """Start class for get_most_recent_messages."""
        try:
            TwitterRecentMessagesUser.__init__(self, **kwargs)
        except Exception as e:
            self.elog(self, e)
        # self.call_list should now contain all dfs

    @classmethod
    def _unsent_telegram_msgs(cls, self, **kwargs):
        """Check for unsent telegram messages."""
        try:
            TwitterUnsentTelegramMsgs.__init__(self, **kwargs)
        except Exception as e:
            self.elog(self, e)

    @classmethod
    def _make_tradeable_messages(cls, self, **kwargs):
        """Make tradeable messages -> return df_msgs."""
        try:
            MakeTradeableMessages.__init__(self, **kwargs)
        except Exception as e:
            self.elog(self, e)

    @classmethod
    def _send_telegram_polls_from_trade(cls, self, **kwargs):
        """Initiate class for SendTelegramPollFromTrade."""
        try:
            SendTelegramPollFromTrade.__init__(self, **kwargs)
        except Exception as e:
            self.elog(self, e)
