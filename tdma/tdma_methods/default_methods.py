"""Default class for handling response objects."""
# %% codecell

import pandas as pd

# %% codecell


class TdmaDefault():
    """TDMA Default Response Handler."""
    nkeys = ['get_transactions', 'get_principals', 'get_orders']

    def __init__(self, api_val, get, **kwargs):
        if api_val in self.nkeys:
            self.df = self._default_norm(self, get, **kwargs)
        elif api_val == 'get_accounts':
            self.df = self._get_accounts(self, get, **kwargs)

    @classmethod
    def _default_norm(cls, self, get, **kwargs):
        """Json normalize."""
        df = pd.json_normalize(get.json())
        return df

    @classmethod
    def _get_accounts(cls, self, get, **kwargs):
        """Get accounts method handler."""
        df = pd.json_normalize(get.json())
        repl_dict = ({'securitiesAccount': 'sA', 'initialBalance': 'iB',
                      'currentBalances': 'cB', 'projectedBalances': 'pB'})
        cols = df.columns
        for key, val in repl_dict.items():
            cols = [col.replace(key, val) for col in cols]
        df.columns = cols

        return df

# %% codecell
