"""Interactive Brokers Workbook."""


# %% codecell
##########################################
from datetime import date
import os.path
import pandas as pd
import time

try:
    from scripts.dev.multiuse.help_class import baseDir, dataTypes, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, dataTypes, getDate

# %% codecell
##########################################


class warrantOps():
    """Warrant analytics stored in local df for frontend."""

    base_dir = baseDir().path
    df = False
    # Date to use for fpath selection
    dt = getDate.query('iex_close')
    dt_cboe = getDate.query('cboe')

    def __init__(self, val):
        self.construct_fpath(self, val, self.dt, self.base_dir)
        self.check_if_exists(self, val)

    @classmethod
    def construct_fpaths(cls, self, val, dt, base_dir):
        """Get correct date. Construct fpath."""
        # Construt test path to see if local file is available
        test_path = f"{self.base_dir}/tickers/warrants/cheapest/_{dt}.gz"
        # If it's not, go one business day backwards
        if not os.path.isfile(test_path) and (dt != self.dt_cboe):
            dt = (dt - BusinessDay(n=1)).date()

        # Fpath Dict
        warrant_fpaths = ({
            'cheapest': f"{base_dir}/tickers/warrants/cheapest/_{dt}.gz",
            'newest': f"{base_dir}/tickers/warrants/newest/_{dt}.gz",
            'top_perf': f"{base_dir}/tickers/warrants/top_perf/_{dt}.gz",
            'worst_perf': f"{base_dir}/tickers/warrants/worst_perf/_{dt}.gz",
            'all': f"{base_dir}/tickers/warrants/all/_{dt}.gz",
            'all_hist': f"{base_dir}/tickers/warrants/all_hist/_{dt}.gz"
        })

        # Assign file path to variable wt_fpath
        try:
            self.wt_fpath = warrant_fpaths[val]
        except KeyError:  # If for some reason val is not defined
            self.wt_fpath = warrant_fpaths['cheapest']

        # System file paths
        self.syms_fpath = f"{base_dir}/tickers/all_symbols.gz"
        # Iex close data file path
        self.iex_eod_fpath = f"{base_dir}/iex_eod_quotes/combined/_{dt}.gz"

        self.dt = dt

    @classmethod
    def check_if_exists(cls, self, val):
        """Check if val meets conds and/or exists locally."""
        if os.path.isfile(self.wt_fpath) and val != 'all_hist':  # If cheap warrant file exists
            wt_df = pd.read_json(self.wt_fpath, compression='gzip')
            cols_to_round = (['ytdChange', 'changePercent', 'iexClose', 'iexOpen',
                              'open', 'close', 'week52High', 'week52Low'])
            try:
                for col in cols_to_round:
                    if col in ('ytdChange', 'changePercent'):
                        wt_df[col] = (wt_df[col] * 100).round(2)
                    else:
                        wt_df[col] = wt_df[col].round(2)
            except KeyError:
                pass
            self.df = wt_df.to_json(orient='records')
        elif os.path.isfile(self.iex_eod_fpath):  # If IEX EOD data exists
            self.if_iex_eod_exists(self, val)

    @classmethod
    def if_iex_eod_exists(self, val):
        """Since iex eod exists, construct local df depending on val param."""
        # Read all symbols
        all_symbols = pd.read_json(self.syms_fpath, compression='gzip')
        wt_list = all_symbols[all_symbols['type'] == 'wt'][['symbol', 'name']]
        wt_list['expDate'] = (pd.to_datetime(wt_list['name'].str[-11:-1],
                                             format='%d/%m/%Y',
                                             errors='coerce').dt.date)
        wt_list['expDate'] = wt_list['expDate'].astype('category')
        wt_syms = all_symbols[all_symbols['type'] == 'wt']['symbol']

        # Read most recent iex data and exclude all non warrants
        iex_df = pd.read_json(self.iex_eod_fpath, compression='gzip')
        # wt_df = iex_df[iex_df['symbol'].isin(wt_syms)]
        wt_df = pd.merge(iex_df, wt_list, on=['symbol']).copy(deep=True)
        wt_df.reset_index(inplace=True, drop=True)

        if val == 'cheapest':
            wt_df = wt_df.sort_values(by=['iexClose'], ascending=True).head(25)
        elif val == 'top_perf':
            wt_df = wt_df.sort_values(by=['ytdChange'], ascending=False).head(25)
        elif val == 'worst_perf':
            wt_df = wt_df.sort_values(by=['ytdChange'], ascending=True).head(25)
        elif val == 'all':  # No change necessary to original dataframe
            pass
        elif val in ('all_hist', 'newest'):
            self.all_hist_or_newest(self, val, wt_syms, wt_df)
        else:
            raise KeyError

        # If self.df is not a dataframe (if val is not all_hist or newest)
        if not isinstance(self.df, pd.DataFrame):
            self.df = wt_df.to_json(orient='records')

    @classmethod
    def all_hist_or_newest(cls, self, val, wt_syms, wt_df):
        """Get all hist or newest."""
        df_list, yr = [], date.today().year
        hist_fpath_base = f"{self.base_dir}/StockEOD/{yr}"
        for sym in wt_syms:
            hist_fpath = f"{hist_fpath_base}/{sym.lower()[0]}/_{sym}.gz"
            try:
                df_list.append(pd.read_json(hist_fpath))
                # iex_get_hist.delay([sym])
            except ValueError:
                # iex_get_hist.delay([sym])
                pass

        all_wt_hist_df = pd.concat(df_list)
        all_wt_hist_df.reset_index(inplace=True, drop=True)

        if val == 'newest':
            wt_ser_10 = (all_wt_hist_df['key'].value_counts()
                         [all_wt_hist_df['key'].value_counts() < 10])
            syms_below_10 = wt_ser_10.index.tolist()
            wt_df = wt_df[wt_df['symbol'].isin(syms_below_10)]

            wt_freq_df = pd.DataFrame(wt_ser_10).reset_index()
            wt_freq_df.rename(columns={'index': 'symbol', 'key': 'freq'}, inplace=True)
            wt_merged_df = wt_df.merge(wt_freq_df, on='symbol')

            wt_merged_df.reset_index(inplace=True, drop=True)
            wt_df = wt_merged_df.copy(deep=True)
            wt_df = wt_df.sort_values(by=['freq'], ascending=True)
        else:
            wt_df = all_wt_hist_df.copy(deep=True)

        # Get historical data for all symbols
        # [iex_get_hist.delay([sym]) for sym in wt_df['symbol'].tolist()]
        wt_df.to_json(self.wt_fpath)

        try:
            wt_df['ytdChange'] = wt_df['ytdChange'].round(2)
            wt_df['changePercent'] = wt_df['changePercent'].round(2)
        except KeyError:
            pass
        # [iex_get_hist.delay([sym]) for sym in sym_list]
        # Return jsonified dataframe for frontend view application
        self.df = wt_df.to_json(orient='records')
