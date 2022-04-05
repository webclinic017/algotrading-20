"""TDMA Batch Master."""
# %% codecell

try:
    from scripts.dev.tdma.batches.daily_options import GetTMDADailyOptions
    from scripts.dev.tdma.tdma_methods.tdma_options_combine import TdmaCombine
    from scripts.dev.tdma.batches.update_my_symbols import TdmaWatchPositionsSymbols
except ModuleNotFoundError:
    from tdma.batches.daily_options import GetTMDADailyOptions
    from tdma.tdma_methods.tdma_options_combine import TdmaCombine
    from tdma.batches.update_my_symbols import TdmaWatchPositionsSymbols


# %% codecell


class TdmaBatchMaster(GetTMDADailyOptions, TdmaCombine,
                      TdmaWatchPositionsSymbols):
    """Master router for TDMA batch functions."""

    batch_list = (['daily_opts', 'combine_options'
                   'update_my_symbols'])

    def __init__(self, method, **kwargs):
        if method == 'daily_opts':
            self._tbm_instantiate_daily_options(self, **kwargs)
        elif method == 'combine_options':
            self._tbm_instantiate_combine_options(self, method, **kwargs)
        elif method == 'update_my_symbols':
            self._tbm_instantiate_my_symbols(self, **kwargs)

    @classmethod
    def _tbm_instantiate_daily_options(cls, self, **kwargs):
        """Instantiate daily options class."""
        GetTMDADailyOptions.__init__(self, **kwargs)

    @classmethod
    def _tbm_instantiate_combine_options(cls, method, self, **kwargs):
        """Instantiate tdma combine options class."""
        TdmaCombine.__init__(self, method, **kwargs)

    @classmethod
    def _tbm_instantiate_my_symbols(cls, self, **kwargs):
        """Instantiate update_my_symbols class."""
        TdmaWatchPositionsSymbols.__init__(self, **kwargs)
