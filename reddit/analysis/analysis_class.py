"""Reddit master analysis."""
# %% codecell

try:
    from scripts.dev.multiuse.help_class import help_print_arg
    from scripts.dev.reddit.analysis.top_symbols_sub import RedditTopSymbolsMentioned
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
    from reddit.analysis.top_symbols_sub import RedditTopSymbolsMentioned


# %% codecell


class RedditAnalysis(RedditTopSymbolsMentioned):
    """Reddit Analysis class."""

    # Gets imported into RedditAPI and instantiated there
    ranalysis_dict = ({})

    def __init__(self, method, **kwargs):
        self._get_class_vars(self, method, **kwargs)
        self._analysis_instantiate_top_symbols(self, method, **kwargs)

    @classmethod
    def _get_class_vars(cls, self, method, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        if self.verbose:
            help_print_arg(f"RedditAnalysis: {str(method)}")

    @classmethod
    def _analysis_instantiate_top_symbols(cls, self, method, **kwargs):
        """Call top symbols class."""
        RedditTopSymbolsMentioned.__init__(self, method, **kwargs)

        # Merge dictionary methods
        self.ranalysis_dict = self.ranalysis_dict | self.rtsm_dict
