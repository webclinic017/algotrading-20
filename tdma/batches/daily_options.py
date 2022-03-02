"""Batch Processes for TDMA API."""
# %% codecell
import time
from datetime import datetime, timedelta
from tqdm import tqdm

try:
    from scripts.dev.tdma.tdma_api.td_api import TD_API
    from scripts.dev.multiuse.symbol_ref_funcs import remove_funds_spacs
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from tdma.tdma_api.td_api import TD_API
    from multiuse.symbol_ref_funcs import remove_funds_spacs
    from multiuse.help_class import getDate
    from api import serverAPI


# %% codecell


class GetTMDADailyOptions():
    """Get daily derivatives for ~ 4000 symbols."""

    def __init__(self, **kwargs):
        symbols = self._get_symbols(self)
        self._all_sym_list = self._start_loop(self, symbols, **kwargs)

    @classmethod
    def _get_symbols(cls, self):
        """Get symbols needed."""
        all_cs = remove_funds_spacs()
        cboe_symref = serverAPI('cboe_symref').df
        symbols = cboe_symref['Underlying'].unique()
        all_cs_opts = all_cs[all_cs['symbol'].isin(symbols)]

        return all_cs_opts

    @classmethod
    def _start_loop(cls, self, symbols, **kwargs):
        """Start loop for derivative symbols."""
        dt = getDate.query('mkt_open')

        all_sym_list = []
        sym_list = []
        reset = datetime.now() + timedelta(seconds=60)
        kwargs = ({'params':
                  {'symbol': '', 'includeQuotes': 'TRUE', 'fromDate': str(dt)}
                   })

        for symbol in tqdm(symbols['symbol']):
            kwargs['params']['symbol'] = symbol
            TD_API(api_val='options_chain', **kwargs)

            if len(sym_list) >= 115 and datetime.now() < reset:
                time.sleep((reset - datetime.now()).seconds)
                reset = datetime.now() + timedelta(seconds=60)
                sym_list = []
            elif datetime.now() > reset:
                reset = datetime.now() + timedelta(seconds=60)
                sym_list = []
                continue
            else:
                sym_list.append(symbol)
                all_sym_list.append(symbol)
                continue

        return all_sym_list
