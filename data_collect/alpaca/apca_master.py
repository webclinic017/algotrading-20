"""Alpaca master class for methods/else."""
# %% codecell

try:
    from scripts.dev.data_collect.alpaca.api_calls.apca_api import ApcaAPI
    from scripts.dev.data_collect.alpaca.batch_calls.all_recent_announcements import AllRecentAnnouncements
    from scripts.dev.data_collect.alpaca.batch_calls.hist_news_loop import HistoricalNewsLoop
except ModuleNotFoundError:
    from data_collect.alpaca.api_calls.apca_api import ApcaAPI
    from data_collect.alpaca.batch_calls.all_recent_announcements import AllRecentAnnouncements
    from data_collect.alpaca.batch_calls.hist_news_loop import HistoricalNewsLoop

# %% codecell


class ApcaMaster(ApcaAPI):
    """Master Alpaca class to instantiate everything else."""
    # ToDo: this needs to be rewritten for the methods to make more sense
    # replacing the batch_ with '' is stupid

    def __init__(self, method, **kwargs):
        if 'stream' in method:
            self._master_streams(self, method, **kwargs)
        elif 'batch' in method:
            self._master_batch_calls(self, method, **kwargs)
        else:
            self._master_api_method(self, method, **kwargs)

    @classmethod
    def _master_streams(cls, self, method, **kwargs):
        """Call stream subclass to start streaming from endpoint."""
        try:
            from app.tasks_stream import apca_news_streaming
            apca_news_streaming.delay()
        except ModuleNotFoundError:
            from data_collect.alpaca.news.real_time import ApcaNewsStream
            ApcaNewsStream.__init__(**kwargs)

    @classmethod
    def _master_batch_calls(cls, self, method, **kwargs):
        """Call master class with api method."""
        method = method.replace('batch_', '')
        # batch_recent_announcements
        if method == 'recent_announcements':
            AllRecentAnnouncements(**kwargs)
        # batch_historical_news
        elif method == 'historical_news':
            HistoricalNewsLoop(**kwargs)

    @classmethod
    def _master_api_method(cls, self, method, **kwargs):
        """Call master class with api method."""
        ApcaAPI.__init__(self, method, **kwargs)
