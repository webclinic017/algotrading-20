"""Reddit API class."""
# %% codecell

try:
    from scripts.dev.reddit.r_api.praw_auth import RedditPrawAuth
    from scripts.dev.reddit.methods.methods_class import RedditMethods
except ModuleNotFoundError:
    from reddit.r_api.praw_auth import RedditPrawAuth
    from reddit.methods.methods_class import RedditMethods

# %% codecell


class RedditAPI(RedditPrawAuth, RedditMethods):
    """Reddit API Class."""

    def __init__(self, method, **kwargs):
        self._rapi_instantiate_praw_auth(self, **kwargs)
        self._rapi_instantiate_methods(self, method, **kwargs)

    @classmethod
    def _rapi_instantiate_praw_auth(cls, self, **kwargs):
        """Instantiate PRAW class and subreddit."""
        RedditPrawAuth.__init__(self, **kwargs)

    @classmethod
    def _rapi_instantiate_methods(cls, self, method, **kwargs):
        """Instantiate Reddit Methods Class."""
        RedditMethods.__init__(self, method, **kwargs)

        if method == 'sub_comments':
            RedditMethods.__init__(self, method='sub_authors', **kwargs)


# %% codecell
