"""Econ data master."""
# %% codecell

try:
    from scripts.dev.data_collect.econ_data.factset import FactSetSources
    from scripts.dev.data_collect.econ_data.fed_fomc import FedFomc
    from scripts.dev.data_collect.econ_data.fed_rss import FedPressRss
    from scripts.dev.multiuse.class_methods import ClsHelp
except ModuleNotFoundError:
    from data_collect.econ_data.factset import FactSetSources
    from data_collect.econ_data.fed_fomc import FedFomc
    from data_collect.econ_data.fed_rss import FedPressRss
    from multiuse.class_methods import ClsHelp


# %% codecell


class EconMaster(FactSetSources, FedFomc, FedPressRss, ClsHelp):
    """Econ master class for FED/covid/pres announcements."""
    # Test all methods with method == 'testing_all'

    # Factset methods: covid, biden, white_house_news
    factset_methods = ['covid', 'biden', 'white_house_news']
    # FOMC Calendar: fomc_calendar
    fomc_methods = ['fomc_calendar']
    # FED speeches + calendar
    fpr_methods = ['fed_speeches', 'fed_testimony', 'fed_press']

    def __init__(self, method, **kwargs):
        self._em_class_vars(self, method, **kwargs)

        if method in self.factset_methods:
            self._em_instantiate_factset_class(self, method, **kwargs)
        elif method in self.fomc_methods:
            self._em_instantiate_fomc_calendar(self, **kwargs)
        elif method in self.fpr_methods:
            self._em_instantiate_fpr_speeches(self, method, **kwargs)

        if self.testing_all:
            self._em_test_all_methods(self, **kwargs)

    @classmethod
    def _em_class_vars(cls, self, method, **kwargs):
        """Econ master class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')
        self.testing_all = kwargs.get('testing_all')
        if method == 'testing_all':
            self.testing_all = True

    @classmethod
    def _em_instantiate_factset_class(cls, self, method, **kwargs):
        """Instantiate factset class with the right method."""
        FactSetSources.__init__(self, method, **kwargs)

    @classmethod
    def _em_instantiate_fomc_calendar(cls, self, **kwargs):
        """Instantiate the Federal reserve meeting calendar class."""
        FedFomc.__init__(self, **kwargs)

    @classmethod
    def _em_instantiate_fpr_speeches(cls, self, method, **kwargs):
        FedPressRss.__init__(self, method, **kwargs)

    @classmethod
    def _em_test_all_methods(cls, self, **kwargs):
        """Econ master - test all methods."""
        kwargs['verbose'], self.verbose = True, True

        for m in self.factset_methods:
            try:
                FactSetSources.__init__(self, m, **kwargs)
            except Exception as e:
                self.elog(self, e)

        for m in self.fomc_methods:
            try:
                FedFomc.__init__(self, **kwargs)
            except Exception as e:
                self.elog(self, e)

        for m in self.fpr_methods:
            try:
                FedPressRss.__init__(self, m, **kwargs)
            except Exception as e:
                self.elog(self, e)
