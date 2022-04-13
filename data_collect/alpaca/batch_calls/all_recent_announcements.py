"""Alpaca Announcements."""
# %% codecell
from datetime import timedelta

try:
    from scripts.dev.data_collect.alpaca.api_calls.apca_api import ApcaAPI
    from scripts.dev.multiuse.help_class import getDate
except ModuleNotFoundError:
    from data_collect.alpaca.api_calls.apca_api import ApcaAPI
    from multiuse.help_class import getDate


# %% codecell

class AllRecentAnnouncements():
    """All corporate actions within the last 2 days."""

    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        params = self._ara_create_dt_params(self, **kwargs)
        self._ara_initiate_ca_sequence(self, params, **kwargs)

    @classmethod
    def _ara_create_dt_params(cls, self, **kwargs):
        """Create date specific parameters for api calls."""
        params = {}
        since = getDate.query('iex_previous') - timedelta(days=1)
        params['since'] = since
        return params

    @classmethod
    def _ara_initiate_ca_sequence(cls, self, params, **kwargs):
        """Start corporate action sequence loop."""
        # Corporate action/announcement list
        ca_list = ['Dividend', 'Merger', 'Spinoff', 'Split']
        kwargs['params'] = params

        for ca in ca_list:
            params['ca_types'] = [ca]
            ApcaAPI(api_val='announcements', **kwargs)

# %% codecell
