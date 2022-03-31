"""Streaming Params Classes."""
# %% codecell

import numpy as np

try:
    from scripts.dev.tdma.streaming.streaming_helpers import TdmaStreamingHelpers
    from scripts.dev.ref_data.symbol_meta_stats import SymbolRefMetaInfo
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from tdma.streaming.streaming_helpers import TdmaStreamingHelpers
    from ref_data.symbol_meta_stats import SymbolRefMetaInfo
    from multiuse.help_class import help_print_arg

# %% codecell


class TdmaStreamingParams():
    """Streaming params for different subscriber channels."""
    # tsp: tdma streaming params

    sp_dict = {}
    tsh = TdmaStreamingHelpers

    def __init__(self, df_gpf, **kwargs):
        self._tsp_get_class_vars(self, **kwargs)
        self.df_m = self._tsp_symbol_ref_meta(self)
        self.sp_dict['sub_quotes'] = self._tsp_sub_quotes(self, df_gpf, "QUOTE")
        self.sp_dict['sub_timesale_equity'] = self._tsp_sub_quotes(self, df_gpf, "TIMESALE_EQUITY")
        self.sp_dict['sub_option'] = self._tsp_sub_options(self, df_gpf, "OPTION")
        self.sp_dict['sub_timesale_options'] = self._tsp_sub_options(self, df_gpf, "TIMESALE_OPTIONS")
        self.request_list = self._tsp_make_request_list(self)

    @classmethod
    def _tsp_get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.requestid = kwargs.get('requestid', 1)

    @classmethod
    def _tsp_symbol_ref_meta(cls, self, n=2500):
        """Get a symbol list of the top n companies by market cap."""
        if self.testing:  # Adjust to 100 if trying to test
            n = 100

        df_meta = (SymbolRefMetaInfo(df_all=True).df
                   .sort_values(by='marketcap')
                   .dropna(subset='marketcap')
                   .head(n)  # default 2500
                   .copy())

        df_m2500 = (df_meta.loc[
                    df_meta.index
                    .isin(df_meta['marketcap']
                          .replace(0, np.NaN)
                          .dropna().index), :]
                    .copy())
        if df_m2500.empty:
            help_print_arg("TSP: df_m2500 empty")

        df_m = df_m2500.copy()
        # Return meta dataframe
        return df_m

    @classmethod
    def _tsp_sub_quotes(cls, self, df_gpf, quote_service):
        """Quote subscription message."""
        if quote_service not in ('QUOTE', "TIMESALE_EQUITY"):
            return {'message': f'quote_service not in params {str(quote_service)}'}

        symbols_as_list = self.df_m.index.tolist()
        num_fields = str(list(range(0, 30, 1)))
        if quote_service == 'TIMESALE_EQUITY':
            num_fields = str(list(range(0, 5, 1)))

        # Add an integer value to requestid
        self.requestid += 1

        sub_quotes = ({
            "service": quote_service,
            "requestid": str(self.requestid),
            "command": "SUBS",
            "account": df_gpf.loc['accountId'][0],
            "source": df_gpf.loc['streamerInfo.appId'][0],
            "parameters": {  # where tsh is TdmaStreamHelpers
                "keys":  self.tsh.remove_exchar(symbols_as_list),
                "fields": self.tsh.remove_exchar(num_fields)}
        })

        # Return sub_quotes request dict
        return sub_quotes

    @classmethod
    def _tsp_sub_options(cls, self, df_gpf, option_service):
        """Quote subscription message."""
        if option_service not in ('OPTION', "TIMESALE_OPTIONS"):
            return {'message': f'option_service not in params {str(option_service)}'}

        symbols_as_list = self.df_m.index.tolist()
        num_fields = str(list(range(0, 42, 1)))
        if option_service == 'TIMESALE_OPTIONS':
            num_fields = str(list(range(0, 5, 1)))

        # Add an integer value to requestid
        self.requestid += 1

        sub_dict = ({
            "service": option_service,
            "requestid": str(self.requestid),
            "command": "SUBS",
            "account": df_gpf.loc['accountId'][0],
            "source": df_gpf.loc['streamerInfo.appId'][0],
            "parameters": {  # where tsh is TdmaStreamHelpers
                "keys":  self.tsh.remove_exchar(symbols_as_list),
                "fields": self.tsh.remove_exchar(num_fields)}
        })

        # Return sub_quotes request dict
        return sub_dict

    @classmethod
    def _tsp_make_request_list(cls, self):
        """Iterate through self.sp_dict and add to request_list."""
        request_list = []
        for key, val in self.sp_dict.items():
            request_list.append(val)

        return request_list

# %% codecell
