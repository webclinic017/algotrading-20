"""Top Level TDMA Class for all methods."""
# %% codecell

try:
    from scripts.dev.tdma.tdma_api.td_api import TD_API
    from scripts.dev.tdma.batches.daily_options import GetTMDADailyOptions
except ModuleNotFoundError:
    from tdma.tdma_api.td_api import TD_API
    from tdma.batches.daily_options import GetTMDADailyOptions


# %% codecell


class TdmaMaster(TD_API):
    """TDMA API Top Level."""

    batch_dict = {'daily_opts': GetTMDADailyOptions}

    def __init__(self, api_val=None, batch_method=None, **kwargs):
        if api_val:
            self._call_td_api(self, api_val, **kwargs)
        if batch_method:
            self._call_batch_method(self, batch_method, **kwargs)

    @classmethod
    def _call_td_api(cls, self, api_val, **kwargs):
        """TD API with api_val."""
        TD_API.__init__(self, api_val=api_val, **kwargs)

    @classmethod
    def _call_batch_method(cls, self, batch_method, **kwargs):
        """Call a batch method for the td api."""
        self.batch_dict[batch_method](**kwargs)


# %% codecell


# %% codecell
