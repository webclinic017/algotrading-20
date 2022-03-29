"""Reddit PRAW Authorization."""
# %% codecell
import os

from dotenv import load_dotenv
import praw

# %% codecell


class RedditPrawAuth():
    """Base class for Reddit auth through PRAW lib."""

    def __init__(self, **kwargs):
        self._load_class_vars(self, **kwargs)
        self.r = self._get_r_base(self)
        self.r_sub = self._get_r_sub(self)

    @classmethod
    def _load_class_vars(cls, self, **kwargs):
        """Load class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.r_sub_name = kwargs.get('r_sub_name', 'wallstreetbets')

        load_dotenv()

        self.client_id = os.environ.get('reddit_client_id', None)
        self.client_secret = os.environ.get('reddit_client_secret', False)
        self.r_user_agent = os.environ.get('reddit_user_agent', False)
        self.r_username = os.environ.get('reddit_username', False)
        self.r_password = os.environ.get('reddit_pass', False)

    @classmethod
    def _get_r_base(cls, self):
        """Get reddit praw base auth class."""
        r = (praw.Reddit(
             client_id=self.client_id,
             client_secret=self.client_secret,
             user_agent=self.r_user_agent,
             username=self.r_username,
             password=self.r_password
             ))
        return r

    @classmethod
    def _get_r_sub(cls, self):
        """Get r subreddit."""
        r_sub = self.r.subreddit(self.r_sub_name)
        return r_sub
