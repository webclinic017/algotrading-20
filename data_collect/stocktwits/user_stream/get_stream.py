"""Get the stocktwits user stream."""
# %% codecell

import requests

try:
    from scripts.dev.multiuse.api_calls import RecordAPICalls
except ModuleNotFoundError:
    from multiuse.api_helpers import RecordAPICalls

# %% codecell


class GetSTUserStream():
    """Get user messages and extract symbols/watch counts."""

    burl = "https://api.stocktwits.com/api/2/streams"

    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        user_list = self._make_user_list(self)
        self.get_list = self._get_messages(self, user_list)

    @classmethod
    def _make_user_list(cls, self):
        """Make and return user list."""
        user_list = (['Nightmare919', 'Law306', 'billythekid123',
                      'TheInvestor23', 'InsiderFinance', 'Sherlock'])
        return user_list

    @classmethod
    def _get_messages(cls, self, user_list):
        """Get user messages."""
        resp_list = []
        for u in user_list:
            user_url = f"{self.burl}/user/{u}.json"
            # Data decoded
            resp_list.append(self._get_ops(self, user_url))

        return resp_list

    @classmethod
    def _get_ops(cls, self, user_url):
        """Data cleaning/getting ops."""
        get = requests.get(user_url)
        if get.status_code < 400:  # Record api call to local file tdma
            RecordAPICalls(get, 'stocktwits')
            return get
        else:  # Record api call to local file tdma
            RecordAPICalls(get, 'stocktwits', **{'verbose': True})
