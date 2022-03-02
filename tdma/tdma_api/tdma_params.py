"""TD Ameritrade Parameters for Func Calls."""
# %% codecell

from datetime import timedelta

try:
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.tdma.tdma_methods.td_clean_options import TD_Clean_Write_Options
except ModuleNotFoundError:
    from multiuse.help_class import getDate
    from tdma.tdma_methods.td_clean_options import TD_Clean_Write_Options

# %% codecell


class TDMA_Param_Dicts():
    """Dictionaries of TDMA Parameters."""

    """
    param: self.dt : base date used
    param: self.tdma_api_endpoints : dict of endpoints
    param: self.td_params : commonly used params for endpoints
    param: self.td_clean_write : dict of functions to clean data
    """

    def __init__(self):
        self._construct_class_params(self)
        self._tdma_api_endpoints(self)
        self._tdma_api_params(self)
        self._tdma_clean_write_functions(self)

    @classmethod
    def _construct_class_params(cls, self):
        """Construct params for use in other class functions."""
        self.dt = getDate.query('iex_eod')

    @classmethod
    def _tdma_api_endpoints(cls, self):
        """Endpoints for TDMA API."""
        burl = 'https://api.tdameritrade.com/v1'

        tdma_api_endpoints = ({
            'options_chain': f'{burl}/marketdata/chains'
        })

        self.tdma_api_endpoints = tdma_api_endpoints

    @classmethod
    def _tdma_api_params(cls, self):
        """Add default parameters for dictionary."""
        to_dt = self.dt + timedelta(weeks=(52 * 2))

        td_params = ({
            'options_chain':
            {'symbol': '', 'includeQuotes': '', 'toDate': str(to_dt)}
        })

        self.td_params = td_params

    @classmethod
    def _tdma_clean_write_functions(cls, self):
        """Dictionary of commonly used cleaning functions."""
        td_clean_dict = ({
            'options_chain': TD_Clean_Write_Options
        })

        self.td_clean_dict = td_clean_dict


# %% codecell
