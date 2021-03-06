"""Top Level TDMA Class for all methods."""
# %% codecell

try:
    from scripts.dev.tdma.tdma_api.td_api import TD_API
    from scripts.dev.tdma.batches.batch_master import TdmaBatchMaster
    from scripts.dev.tdma.streaming.tdma_streaming import TdmaStreaming
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from tdma.tdma_api.td_api import TD_API
    from tdma.batches.batch_master import TdmaBatchMaster
    from tdma.streaming.tdma_streaming import TdmaStreaming
    from multiuse.help_class import help_print_arg

# %% codecell


class TdmaMaster(TD_API, TdmaStreaming, TdmaBatchMaster):
    """TDMA API Top Level."""

    def __init__(self, api_val=None, batch_method=None, streaming=None, **kwargs):
        if api_val:
            self._call_td_api(self, api_val, **kwargs)
        if batch_method:
            self._call_batch_method(self, batch_method, **kwargs)
        if streaming:
            self._call_streaming(self, streaming, **kwargs)

        if not api_val and not batch_method and not streaming:
            help_print_arg("TdmaMaster: no method selected")

    @classmethod
    def _call_td_api(cls, self, api_val, **kwargs):
        """TD API with api_val."""
        TD_API.__init__(self, api_val=api_val, **kwargs)

    @classmethod
    def _call_batch_method(cls, self, batch_method, **kwargs):
        """Call a batch method for the td api."""
        if batch_method == 'combine_options':
            kwargs['use_dask'] = True
            # kwargs['method'] = 'options_chain'

        TdmaBatchMaster.__init__(self, batch_method, **kwargs)

    @classmethod
    def _call_streaming(cls, self, streaming, **kwargs):
        """Call TdmaStreaming class."""
        TdmaStreaming.__init__(self, **kwargs)


# %% codecell


# %% codecell
