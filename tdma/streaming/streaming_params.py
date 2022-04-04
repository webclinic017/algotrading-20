"""Streaming Params Classes."""
# %% codecell
from itertools import product

import numpy as np

try:
    from scripts.dev.tdma.streaming.streaming_helpers import TdmaStreamingHelpers
    from scripts.dev.ref_data.symbol_meta_stats import SymbolRefMetaInfo
    from scripts.dev.multiuse.help_class import help_print_arg, getDate
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from tdma.streaming.streaming_helpers import TdmaStreamingHelpers
    from ref_data.symbol_meta_stats import SymbolRefMetaInfo
    from multiuse.help_class import help_print_arg, getDate
    from api import serverAPI
# %% codecell


class TdmaStreamingParams():
    """Streaming params for different subscriber channels."""
    # tsp: tdma streaming params

    sp_dict = {}
    tsh = TdmaStreamingHelpers

    def __init__(self, df_gpf, **kwargs):
        self._tsp_get_class_vars(self, **kwargs)
        self.df_m = self._tsp_symbol_ref_meta(self)
        self.cboe_sym_list = self._tsp_cboe_symlist(self)
        self.sp_dict['sub_quote'] = self._tsp_sub_quotes(self, df_gpf, "QUOTE")
        self.sp_dict['sub_timesale_equity'] = self._tsp_sub_quotes(self, df_gpf, "TIMESALE_EQUITY")
        self.sp_dict['sub_option'] = self._tsp_sub_options(self, df_gpf, "OPTION")
        self.sp_dict['sub_timesale_options'] = self._tsp_sub_options(self, df_gpf, "TIMESALE_OPTIONS")
        self.sp_dict = self.sp_dict | self._tsp_sub_actives(self, df_gpf)

        self.request_list = self._tsp_make_request_list(self, **kwargs)

    @classmethod
    def _tsp_get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.requestid = kwargs.get('requestid', 1)

        self.only_request_list = kwargs.get('only_request_list', [])

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
    def _tsp_cboe_symlist(cls, self, n=2500):
        """Get symlist from cboe to use for options."""
        if self.testing:
            n = 100
        dt = getDate.query('iex_eod')
        df_cboe = serverAPI('cboe_symref').df
        # Convert strike column to integer
        df_cboe['strike'] = (df_cboe['strike']
                             .astype(np.float64)
                             .replace(0, np.NaN)
                             .dropna()
                             .astype('int'))
        # Get list of years to validate
        yr_suf = int(str(dt.year)[-2:])
        year_list = list(range(yr_suf, yr_suf+3, 1))
        # The "right" year
        cboe_ryear = (df_cboe[df_cboe['yr']
                      .astype('int')
                      .isin(year_list)].copy())
        # Convert columns from category dtype to string
        col_list = ['Underlying', 'side', 'strike', 'yr', 'mo', 'yr']
        col_con_dict = {col: 'string' for col in col_list}
        cboe_ryear = cboe_ryear.astype(col_con_dict)
        # Create new expiration date column
        cboe_ryear['expirationDate'] = (cboe_ryear['mo']
                                        + cboe_ryear['day']
                                        + cboe_ryear['yr'])
        # Do regex replacement on column strike
        cboe_ryear['strike'] = (cboe_ryear['strike']
                                .replace('\.0', '', regex=True))
        # Format for the TDMA symbols
        cboe_ryear['tdma_syms'] = (cboe_ryear['Underlying']
                                   + '_' + cboe_ryear['expirationDate']
                                   + cboe_ryear['side']
                                   + cboe_ryear['strike'])
        if self.testing:
            syms_to_get = ['AAPL', 'SPY', 'TSLA']
            cboe_ryear = (cboe_ryear[cboe_ryear['Underlying']
                          .isin(syms_to_get)].copy())
        cboe_symlist = cboe_ryear['tdma_syms'].drop_duplicates().tolist()
        if not cboe_symlist:
            help_print_arg("TdmaStreamingParams: _tsp_cboe_symlist: Error getting cboe_symlist")

        return cboe_symlist

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

        # syms_fmt = ','.join(symbols_as_list)
        # syms_fmt =  self.tsh.remove_exchar(num_fields)
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
                "keys":  self.tsh.remove_exchar(self.cboe_sym_list),
                "fields": self.tsh.remove_exchar(num_fields)}
        })

        # Return sub_quotes request dict
        return sub_dict

    @classmethod
    def _tsp_sub_actives(cls, self, df_gpf):
        """Get actives for each request."""
        active_list = (['ACTIVES_NASDAQ', 'ACTIVES_NYSE', 'ACTIVES_OTCBB'])
        opt_list = (['OPTS-DESC', 'CALLS-DESC', 'PUTS-DESC',
                     'CALLS', 'PUTS', 'OPTS'])
        # opt_tm_list = ['ALL', '3600', '1800', '600', '300', '60']
        opt_tm_list = ['60']
        pre_fmt = list(product(iter(opt_list), iter(opt_tm_list)))
        fmt = [(f"{key}-{val}").replace(' ', '') for key, val in pre_fmt]

        all_actives_dict = {}

        # for service in active_list:
        #    pass
        service = 'ACTIVES_OPTIONS'
        for opt in opt_list:
            # Add an integer value to requestid
            self.requestid += 1
            sub_dict = ({
                "service": service,
                "requestid": str(self.requestid),
                "command": "SUBS",
                "account": df_gpf.loc['accountId'][0],
                "source": df_gpf.loc['streamerInfo.appId'][0],
                "parameters": {
                    "keys": self.tsh.remove_exchar(fmt),
                    "fields": "0,1",
                    }
                }).copy()
            all_actives_dict[f"sub_{service.lower()}_{opt.lower()}"] = sub_dict

        # Redining service as the variable
        for service in active_list:
            # Add an integer value to requestid
            self.requestid += 1
            serv_suf = service.split('_')[-1]  # Get suffix from service
            sub_dict = ({
                "service": service,
                "requestid": str(self.requestid),
                "command": "SUBS",
                "account": df_gpf.loc['accountId'][0],
                "source": df_gpf.loc['streamerInfo.appId'][0],
                "parameters": {
                    "keys": f"{serv_suf}-60",
                    "fields": "0,1"
                }
            }).copy()
            all_actives_dict[f"sub_{service.lower()}_60"] = sub_dict

        return all_actives_dict


    @classmethod
    def _tsp_make_request_list(cls, self, **kwargs):
        """Iterate through self.sp_dict and add to request_list."""
        request_list = []
        for key, val in self.sp_dict.items():
            request_list.append(val)

        if self.only_request_list:
            request_list = []
            for key, val in self.sp_dict.items():
                if key in self.only_request_list:
                    request_list.append(val)
                elif 'ACTIVES_OPTIONS' in self.only_request_list:
                    if 'ACTIVES' in key:
                        request_list.append(val)

        return request_list

# %% codecell
