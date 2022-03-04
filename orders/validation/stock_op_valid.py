"""Stock Order Params Validation."""
# %% codecell

try:
    from scripts.dev.multiuse.help_class import help_print_arg
    from scripts.dev.multiuse.symbol_ref_funcs import remove_funds_spacs
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
    from multiuse.symbol_ref_funcs import remove_funds_spacs

# %% codecell


class StockOrderParamsValidation():
    """Validate order params against accepted values."""
    # ex: p_string = 'open_long_stock_15_aapl_market_today'

    def __init__(self, p_string, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        pdict = self._construct_pdict(self, **kwargs)
        self.result = self._validate_params(self, p_string, pdict, **kwargs)

    @classmethod
    def _construct_pdict(cls, self, **kwargs):
        """Construct acceptable parameters dict."""
        pdict = ({'Action': ['open', 'close', 'add', 'reduce'],
                  'direction': ['long'],  # Disable short anything
                  'assetType': ['stock'],
                  'Quantity': list(range(0, 5000, 1)),
                  'Symbol': remove_funds_spacs(unique_syms=True),
                  'OrderType':  (['market', 'limit', 'stop', 'stop_limit',
                                  'trailing_stop', 'market_on_close']),
                  'Duration': ['day', 'gtc', 'fok']
                  })  # ^ GOOD_TILL_CANCEL, fill_or_kill
        return pdict

    @classmethod
    def _validate_params(cls, self, p_string, pdict, **kwargs):
        """Validate params given acceptable values."""
        if self.verbose:
            help_print_arg(f"OrderParamsValidation: {p_string}")
        result = True

        params = p_string.split('_')
        keys = list(pdict.keys())
        for n, p in enumerate(params):
            pk = pdict[keys[n]]

            if p.lower() in pk:
                continue
            elif keys[n] == 'Quantity':
                try:
                    if int(p) in pk:
                        continue
                except ValueError:
                    result = self._e_msgs(self, keys[n], p)
            elif keys[n] == 'Symbol':
                if p.upper() in pk:
                    continue
                else:
                    result = self._e_msgs(self, keys[n], p)
            else:
                result = self._e_msgs(self, keys[n], p)
        # If any the parameters failed, return False and don't execute
        return result

    @classmethod
    def _e_msgs(cls, self, name, param):
        """Error messages."""
        p = str(param)
        if name == 'Quantity':
            print(f'Cant convert p {p} to integer')
        elif name == 'Symbol':
            print(f"Symbol p {p} not in symbol list")
        else:
            print(f"Param: {p} not in accepted param for key {name}")

        return False
