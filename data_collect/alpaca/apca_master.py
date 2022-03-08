"""Alpaca master class for methods/else."""
# %% codecell

try:
    from scripts.dev.data_collect.alpaca.api_calls.apca_api import ApcaAPI
except ModuleNotFoundError:
    from data_collect.alpaca.api_calls.apca_api import ApcaAPI

# %% codecell


class ApcaMaster(ApcaAPI):
    """Master Alpaca class to instantiate everything else."""

    def __init__(self, method, **kwargs):
        if 'stream' in method:
            self._master_streams(self, method, **kwargs)
        else:
            self._master_api_method(self, method, **kwargs)

    @classmethod
    def _master_api_method(cls, self, method, **kwargs):
        """Call master class with api method."""
        ApcaAPI.__init__(self, method, **kwargs)

    @classmethod
    def _master_streams(cls, self, method, **kwargs):
        """Call stream subclass to start streaming from endpoint."""
        try:
            from app.tasks_stream import apca_news_streaming
            apca_news_streaming.delay()
        except ModuleNotFoundError:
            from data_collect.alpaca.news.real_time import ApcaNewsStream
            ApcaNewsStream.__init__()
