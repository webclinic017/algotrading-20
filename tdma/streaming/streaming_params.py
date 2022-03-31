"""Streaming Params Classes."""
# %% codecell

import numpy as np

try:
    from scripts.dev.tdma.streaming.streaming_helpers import TdmaStreamingHelpers
    from scripts.dev.ref_data.symbol_meta_stats import SymbolRefMetaInfo
except ModuleNotFoundError:
    from tdma.streaming.streaming_helpers import TdmaStreamingHelpers
    from ref_data.symbol_meta_stats import SymbolRefMetaInfo

# %% codecell


class TdmaStreamingParams():
    """Streaming params for different subscriber channels."""
    # tsp: tdma streaming params

    sp_dict = {}
    tsh = TdmaStreamingHelpers

    def __init__(self, df_gpf, **kwargs):
        self._tsp_get_class_vars(self, **kwargs)
        self.df_m = self._tsp_symbol_ref_meta(self)
        self.sp_dict['sub_quotes'] = self._tsp_sub_quotes(self, df_gpf)
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
        df_meta = (SymbolRefMetaInfo(df_all=True).df
                   .sort_values(by='marketcap')
                   .head(n)  # default 2500
                   .copy())

        df_m2500 = (df_meta.loc[
                    df_meta.index
                    .isin(df_meta['marketcap']
                          .replace(0, np.NaN)
                          .dropna().index), :]
                    .copy())

        df_m = df_m2500.copy()
        # Return meta dataframe
        return df_m

    @classmethod
    def _tsp_sub_quotes(cls, self, df_gpf):
        """Quote subscription message."""
        symbols_as_list = self.df_m.index.tolist()
        num_fields = str(list(range(0, 30, 1)))

        sub_quotes = ({
            "service": "QUOTE",
            "requestid": str(self.requestid),
            "command": "SUBS",
            "account": df_gpf.loc['accountId'][0],
            "source": df_gpf.loc['streamerInfo.appId'][0],
            "parameters": {  # where tsh is TdmaStreamHelpers
                "keys":  self.tsh.remove_exchar(symbols_as_list),
                "fields": self.tsh.remove_exchar(num_fields)}
        })
        # Add an integer value to requestid
        self.requestid += 1
        # Return sub_quotes request dict
        return sub_quotes

    @classmethod
    def _tsp_make_request_list(cls, self):
        """Iterate through self.sp_dict and add to request_list."""
        request_list = []
        for key, val in self.sp_dict.items():
            request_list.append(val)

        return request_list

# %% codecell
