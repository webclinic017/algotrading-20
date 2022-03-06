"""Stocktwits Master Class."""
# %% codecell

try:
    from scripts.dev.data_collect.stocktwits.user_stream.stream import StockTwitsUserStream
    from scripts.dev.data_collect.stocktwits.user_stream.get_stream import GetSTUserStream
    from scripts.dev.data_collect.stocktwits.trending.st_trending import StTrending
except ModuleNotFoundError:
    from data_collect.stocktwits.user_stream.stream import StockTwitsUserStream
    from data_collect.stocktwits.user_stream.get_stream import GetSTUserStream
    from data_collect.stocktwits.trending.st_trending import StTrending


# %% codecell


class StockTwitsMaster(GetSTUserStream, StockTwitsUserStream):
    """Master object for all stocktwits methods."""

    def __init__(self, user_stream=False, trending=False, **kwargs):
        if user_stream:
            self._call_user_stream(self, **kwargs)
        if trending:
            self.df = self._call_trending(self, **kwargs)

    @classmethod
    def _call_user_stream(cls, self, **kwargs):
        """Call ST user stream."""
        get_stream = GetSTUserStream()
        for get in get_stream.get_list:
            StockTwitsUserStream(get)

    @classmethod
    def _call_trending(cls, self, **kwargs):
        """Stocktwits Trending."""
        stt = StTrending()
        return stt.df
