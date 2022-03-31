"""Reddit master class."""
# %% codecell
import logging
import pandas as pd

try:
    from scripts.dev.reddit.r_api.reddit_api import RedditAPI
    from scripts.dev.reddit.analysis.analysis_class import RedditAnalysis
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from reddit.r_api.reddit_api import RedditAPI
    from reddit.analysis.analysis_class import RedditAnalysis
    from multiuse.help_class import help_print_arg

# %% codecell


class RedditMaster(RedditAPI, RedditAnalysis):
    """Reddit master (top level) processes."""

    def __init__(self, method, **kwargs):
        self._rm_get_class_vars(self, method, **kwargs)

        self._call_analysis(self, method, **kwargs)
        self._instantiate_rapi(self, method, **kwargs)

        if method == '':
            self._print_all_methods(self, **kwargs)

    @classmethod
    def _rm_get_class_vars(cls, self, method, **kwargs):
        """RM (Reddit Master) class variables."""
        self.testing = kwargs.get('testing', False)
        self.verbose = kwargs.get('verbose', False)
        self.skip_write = kwargs.get('skip_write', False)
        r_sub_name = kwargs.get('r_sub_name', False)

        if method == '':
            self.skip_write = True
            self.df_dp = pd.DataFrame()

        if not r_sub_name:
            msg = """RedditMaster: no subreddit name specified.
                  defaulting to WallStreetBets"""
            help_print_arg(msg)

        if self.testing:
            self._rm_log_debug(self)

    @classmethod
    def _rm_log_debug(cls, self):
        """RM (reddit master) log level debug."""
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        for logger_name in ("praw", "prawcore"):
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.DEBUG)
            logger.addHandler(handler)

    @classmethod
    def _call_analysis(cls, self, method, **kwargs):
        """Call analysis class."""
        RedditAnalysis.__init__(self, method, **kwargs)

    @classmethod
    def _instantiate_rapi(cls, self, method, **kwargs):
        """Instantiate reddit api class."""
        RedditAPI.__init__(self, method, **kwargs)

    @classmethod
    def _print_all_methods(cls, self, **kwargs):
        """Print all methods."""
        dict_all = self.rmethods_dict | self.ranalysis_dict
        print(str(dict_all))



# %% codecell
