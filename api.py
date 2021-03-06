"""Get data from server through API."""
# %% codecell
####################################
import json
from json import JSONDecodeError
from io import BytesIO
from pathlib import Path

import pandas as pd
import numpy as np
import requests
from tqdm import tqdm

try:
    from scripts.dev.multiuse.help_class import (getDate, baseDir,
                                                 help_print_arg, write_to_parquet)
except ModuleNotFoundError:
    from multiuse.help_class import (getDate, baseDir,
                                     help_print_arg, write_to_parquet)

# %% codecell
####################################


def make_url_dict():
    """Make the url dict."""
    url_dict = ({
        'all_symbols': '/symbols/all',
        'apca_all': '/data/apca/all',
        'apca_recent_announcements': '/data/apca/announcements',
        'apca_news_realtime': '/data/apca/news/realtime',
        # Analyst price changes
        'analyst_recs_scraped': '/data/company_stats/analyst_recs/scraped',
        # This is for analyst **EARNINGS** estimates
        'analyst_ests_all': '/data/company_stats/analyst_ests/all',
        'cs_top_vol': '/scans/vol/avg',
        'cboe_mmo_raw': '/cboe/mmo/raw',
        'cboe_mmo_top': '/cboe/mmo/top',
        'cboe_mmo_syms': '/cboe/mmo/syms',
        'cboe_mmo_exp_all': '/cboe/mmo/explore/all',
        'cboe_mmo_exp_last': '/cboe/mmo/explore/last',
        'cboe_dump_last': '/data/cboe/dump/last',
        'cboe_dump_all': '/data/cboe/dump/all',
        'cboe_symref': '/data/cboe/symref/parq',
        'cboe_symref_all': '/data/cboe/symref/all',
        'cboe_intraday_eod': '/data/cboe/intraday/eod/last',
        'cboe_intraday_intra': '/data/cboe/intraday/intraday/last',
        'company_meta': '/data/companies/meta',
        'e_fix_intraday_dataframes': '/data/errors/fix_intraday_dataframes',
        'errors_iex_intraday_1min': '/data/errors/clean_iex_1min',
        'iex_quotes_raw': '/prices/eod/all',
        'iex_comb_today': f"/prices/combined/{getDate.query('cboe')}",
        'iex_intraday_m1': '/data/hist/intraday/minute_1/all',
        'gz_file_sizes': '/data/ref_data/get_sizes',
        'fpath_list': '/data/ref_data/fpath_list',
        'file_sizes': '/data/ref_data/data_files_sizes',
        'missing_dates_less': '/data/hist/missing_dates/less_than_20',
        'missing_dates_all': '/data/hist/missing_dates/all',
        'missing_dates_null': '/data/hist/missing_dates/null',
        'ml_training': '/data/ml/subset',
        # ML training data but just my symbols
        'ml_training_mysyms': '/data/ml/training/mysyms',
        # ML training full
        'ml_training_full': '/data/ml/training/ml_training_full',
        'my_symbols': '/data/my/symbols',
        'new_syms_today': '/symbols/new/today',
        'new_syms_all': '/symbols/new/all',
        'otc_syms': '/symbols/otc',
        'syms_new_mr': '/data/symbols/new/mr',
        'sec_ref': '/data/sec/ref',
        'sec_inst_holdings': '/data/sec/institutions',
        'sec_master_mr': '',
        'sec_master_all': '/data/sec/master_idx/all/False',
        'sec_rss_latest': '/data/sec/rss/latest',
        'sec_rss_all': '/data/sec/rss/all',
        'sector_perf': '/data/hist/sector_perf/mr',
        'stats_combined': '/data/stats/combined',
        'st_stream': '/stocktwits/user_stream',
        'stocktwits_trending': '/data/stocktwits/trending',
        'st_trend_all': '/stocktwits/trending/all',
        'st_trend_today': '/stocktwits/trending/today/explore',
        'st_watch': '/stocktwits/watchlist',
        'stock_data': '/symbols/data',
        'stock_close_prices': '/data/hist/daily/all',
        # Stocks EOD combined all
        'stock_close_cb_all': '/data/hist/daily/cb_all',
        # Stocks EOD combined all column subset
        'stock_close_cols_subset': '/data/hist/daily/cols_subset',
        'stock_close_test': '/data/hist/daily/test',
        'reddit_data': '/data/reddit',  # /{subreddit}/{method}
        'treasuries': '/econ/treasuries',
        'twitter_get_max': '/redo/twitter/max_hist',
        'twitter_errors': '/data/twitter/errors',
        'twitter_hist_all': '/data/twitter/tweets/all',
        'twitter_trade_signals': '/data/twitter/trade_signal/all',
        'twitter_trades_all': '/data/twitter/trades/all',
        'twitter_user_ref': '/data/twitter/user_ref',
        'twitter_logs': '/data/twitter/logs',
        'telegram_polls': '/data/telegram/polls',
        'tdma_movers': '/data/tdma/movers',
        'tdma_streaming': '/data/tdma/streaming',
        # TDMA derivatives all combined
        'tdma_dervs_all_cb': '/data/tdma/dervs/all_combined',
        'tdma_dervs_cb': '/data/tdma/dervs/most_recent_combined',
        'tdma_logs': '/data/tdma/logs',
        'yoptions_daily': '/data/yfinance/derivs/combined/daily',
        'yoptions_all': '/data/yfinance/derivs/combined/all',
        'yoptions_temp': '/data/yfinance/derivs/temp',
        'yoptions_unfin': '/data/yfinance/derivs/unfinished',
        'yoptions_stock': '/data/yfinance/derivs/stock',
        'yinfo_all': '/data/yfinance/info/all',
        'redo': ''
    })

    return url_dict


class serverAPI():
    """Methods for server API endpoints."""

    url, df, get = None, None, None
    base_url = "https://algotrading.ventures/api/v1"
    url_dict = make_url_dict()
    params = {}

    # Data to conacatenate
    concat = ['st_trend', 'cboe_mmo_top']

    def __init__(self, which, **kwargs):
        self._api_unpack_kwargs(self, **kwargs)
        self._api_check_params(self, which, **kwargs)
        df = self._api_get_data(self, which)

        if isinstance(df, pd.DataFrame):
            if df.shape[0] == 0:
                print(self.url)

        self.df = df

    @classmethod
    def _api_unpack_kwargs(cls, self, **kwargs):
        """Unpack kwargs and class variables."""
        self.verbose = kwargs.get('verbose')
        self.symbol = kwargs.get('symbol')

        self.service = kwargs.get('service')

    @classmethod
    def _api_check_params(cls, self, which, **kwargs):
        """Check passed parameters and urls."""
        refresh, val = None, None
        if which in ('sec_master_mr'):
            if 'refresh' in kwargs.keys():
                refresh = kwargs['refresh']
            if 'val' in kwargs.keys():
                val = kwargs['val']

            self.url_dict[which] = f"/sec/data/master_idx/{val}/{refresh}"
        # Redo (views_misc) test param addition
        elif which == 'redo' and 'val' in kwargs.keys():
            val = kwargs['val']
            if 'test' in kwargs.keys():
                print('Test in kwargs.keys()')
                val = f"{val}_test_task_redo"

            self.url_dict[which] = f"/redo/functions/{val}"

        elif which in ('stock_data', 'yoptions_stock') and self.symbol:
            self.url_dict[which] = f"{self.url_dict[which]}/{self.symbol}"

        # Twitter get_max_history with parameter addition
        elif which == 'twitter_get_max' and 'username' in kwargs.keys():
            username = kwargs['username']

            if username not in self.url_dict[which]:
                self.url_dict[which] = f"{self.url_dict[which]}/{username}"

        # Tdma streaming with added service
        elif which == 'tdma_streaming' and self.service:
            service = self.service  # redefine service for convenience
            slist = ['quote', 'options', 'timesale_equity', 'timesale_options']
            # Validate service against possible options
            if service.lower() not in slist:
                help_print_arg(f"tdma_streaming service: {service} not in slist")
            # If service not in url, add it
            if service not in self.url_dict[which]:
                self.url_dict[which] = f"{self.url_dict[which]}/{service}"

        # Apply default params for reddit api call
        elif which == 'reddit_data':
            subreddit = kwargs.get('subreddit', 'wallstreetbets')
            method = kwargs.get('method', 'sub_comments')

            if self.url_dict[which] == '/data/reddit':
                self.url_dict[which] = f"{self.url_dict[which]}/{subreddit}/{method}"
            print(self.url)

        elif which == 'stocktwits_trending':
            days = kwargs.get('days', 5)
            if self.url_dict[which] == '/data/stocktwits/trending':
                self.url_dict[which] = f"{self.url_dict[which]}/{days}"

        elif which in ('stock_close_test', 'stock_close_cols_subset'):
            default_cols = (['date', 'symbol', 'fOpen', 'fHigh',
                             'fLow', 'fClose', 'fVolume'])
            columns = kwargs.get('columns', default_cols)
            self.params['columns'] = json.dumps(columns)

        elif which == 'apca_recent_announcements':
            ca_list = ['dividends', 'mergers', 'spinoffs', 'splits']
            action = kwargs.get('action')
            if action not in ca_list:
                help_print_arg(f"""serverAPI - apca_recent_announcements action
                               {str(action)} not in {str(ca_list)}""")
            if self.url_dict[which] == '/data/apca/announcements':
                self.url_dict[which] = f"{self.url_dict[which]}/{action}"

    @classmethod
    def _api_get_data(cls, self, which):
        """Get data from server."""
        df, get_json = None, None
        url = f"{self.base_url}{self.url_dict[which]}"

        if self.params and self.verbose:  # Print parameters if they exist
            print(f"serverAPI: {str(which)} {str(self.params)}")
            # self.params = json.dumps(self.params)
        # Submit get request
        get = requests.get(url, params=self.params)
        # Store get request under class variable
        self.get = get

        if get.status_code < 400:
            try:
                get_json = json.load(BytesIO(get.content))
            except JSONDecodeError:
                get_json = get.json()
            except UnicodeDecodeError:
                try:
                    df = pd.read_parquet(BytesIO(get.content))
                except OSError:
                    df = pd.read_pickle(BytesIO(get.content))
        else:
            msg = f"Status code failed {get.status_code} with url {url}"
            help_print_arg(msg)
            help_print_arg('Storing get request under serverAPI.get')
            self.get = get

        if not isinstance(df, pd.DataFrame):
            if not get_json:
                self.get = get
                help_print_arg('Storing get request under serverAPI.get')
            else:
                df = get_json

        # If data type needs to be looped/concatenated
        if which in self.concat:
            df = self.concat_data(self, df)
            # Clean stocktwits trending data
            if which == 'st_trend':
                df = self._clean_st_trend(self, df)
        elif which in ('cboe_mmo_exp_all', 'cboe_mmo_exp_last'):
            df = self._mmo_explore_all(self, df)
        elif which == 'stock_data':
            df = pd.read_json(df['iex_hist']).copy(deep=True)
        elif which == 'iex_intraday_m1':
            # df = self._iex_intraday_m1(self, df)
            pass
        elif which == 'apca_news_realtime':
            df = self._clean_apca_news(self, df)
        elif which == 'sec_rss_all':
            df = self._clean_sec_rss_all(self, df)
        elif which == 'sec_ref':
            df = self._get_clean_sec_ref(self, df)
        else:
            # Convert to dataframe
            df = pd.DataFrame(df)

        return df

    @classmethod
    def concat_data(cls, self, dict):
        """Loop through and concatenate dataframes."""
        # Convert all keys to dataframes
        for key in dict.keys():
            dict[key] = pd.DataFrame(dict[key])

        # Get a flattened list of dataframes
        items = list(dict.values())
        # Concatenate dataframes
        this_df = pd.concat(items)
        this_df.reset_index(inplace=True, drop=True)

        return this_df

    @classmethod
    def _iex_intraday_m1(cls, self, df):
        """Write to local file structure."""
        cols_to_keep = ['symbol', 'dtime', 'date', 'minute', 'exchangeType']
        df_cols = df.columns
        mkt_cols = [col for col in df_cols if 'market' in str(col)]
        cols_to_keep = cols_to_keep + mkt_cols
        cols_to_keep = [col for col in cols_to_keep if col in df_cols]
        df_m1 = df[cols_to_keep].copy()
        # df_m1.rename(columns={'sym': 'symbol'}, inplace=True)

        df_m1['year'] = df_m1['date'].dt.year
        df_idx_m1 = df_m1.set_index(['symbol', 'year'])
        sym_list = df_idx_m1.index.get_level_values('symbol').unique()
        yr_list = df_idx_m1.index.get_level_values('year').unique()

        bpath = Path(baseDir().path, 'intraday', 'minute_1')

        for sym in tqdm(sym_list):
            for yr in yr_list:
                df_sym = df_idx_m1.loc[sym, yr].copy()
                sym = str(sym)
                fpath = bpath.joinpath(str(yr), sym[0].lower(), f"_{sym}.parquet")
                write_to_parquet(df_sym.reset_index(), fpath)

        return df_m1

    @classmethod
    def _clean_st_trend(cls, self, df):
        """Clean and return st_trending data."""
        df.dropna(axis=0, inplace=True)
        # Convert to timestamp and get date and hour
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour

        # Convert columns to relevant data type
        df['hour'] = df['hour'].astype(np.uint8)
        df['id'] = df['id'].astype(np.uint16)
        df['symbol'] = df['symbol'].astype('category')

        df['watchlist_count'] = df['watchlist_count'].astype(np.uint32)
        df.rename(columns={'watchlist_count': 'wCount'}, inplace=True)

        return df

    @classmethod
    def _mmo_explore_all(cls, self, df):
        """Return dict of processed/converted dataframes for each CBOE day."""
        cboe_dict = {}

        for key in df.keys():
            key_mod = key[-13:-3]
            try:
                if cboe_dict[key_mod].shape[0] > 1:
                    key_df = pd.DataFrame(df[key])
                    cboe_dict[key_mod] = pd.concat([cboe_dict[key_mod], key_df])
            except KeyError:
                cboe_dict[key_mod] = pd.DataFrame(df[key])

        for key in cboe_dict.keys():
            cboe_dict[key]['Cboe ADV'] = cboe_dict[key]['Cboe ADV'].where(cboe_dict[key]['Cboe ADV'] != 0, 1)
            cboe_dict[key]['vol/avg'] = (cboe_dict[key]['totVol'] / cboe_dict[key]['Cboe ADV']).astype(np.float16)
            cboe_dict[key] = cboe_dict[key].sort_values(by=['vol/avg', 'totVol'], ascending=False).head(150)
            cboe_dict[key].reset_index(inplace=True, drop=True)

        return cboe_dict

    @classmethod
    def _clean_apca_news(cls, self, df):
        """Clean apca news and return dataframe."""
        df['crypto'] = (df['headline'].str.contains(
                               'crypto', regex=True, case=False))

        df['created_at'] = (pd.to_datetime(df['created_at'])
                              .dt.tz_convert('US/Eastern'))
        df['updated_at'] = (pd.to_datetime(df['updated_at'])
                              .dt.tz_convert('US/Eastern'))
        df.drop(columns=['T'], inplace=True)

        return df

    @classmethod
    def _clean_sec_rss_all(cls, self, df):
        """Clean and return sec_rss_all df."""
        if isinstance(df, list):
            df = pd.DataFrame(df).copy()

        try:
            print(f"Original columns are {str(df.columns)}")
        except AttributeError:
            print(type(df))

        df.drop(columns=['index'], inplace=True)
        df.rename(columns={'CIK': 'cik'}, inplace=True)

        dtmap = {'EST': 'US/Eastern', 'EDT': 'US/Central'}
        df['pubDate'] = (df['pubDate'].str.replace(',+', '', regex=True)
                                      .replace(dtmap, regex=True)
                                      .str.strip())
        pdFormat = '%a %d %b %Y %H:%M:%S %Z'
        df['pubDate'] = (pd.to_datetime(
                         df['pubDate'], format=pdFormat, utc=True))
        # Convert to timestamp
        df['dt'] = df['pubDate'].apply(lambda x: x.timestamp())

        return df

    @classmethod
    def _get_clean_sec_ref(cls, self, df):
        """Get and clean sec reference data."""
        if isinstance(df, list):
            df = pd.DataFrame(df).copy()

        df['cik'] = (df['CIK'].astype('str')
                     .str.zfill(10).astype('category'))
        df.drop(columns=['CIK'], inplace=True)

        return df












# %% codecell
####################################


class GetTrainingData():
    """Get training data from api."""
    baseUrl = 'https://algotrading.ventures/api/v1/data/ml'

    def __init__(self):
        self.rep_training = self._get_data(self, 'training')
        self.rep_ref = self._get_data(self, 'ref')

        self.df = self._convert_to_df(self, self.rep_training)
        self.df_ref = self._convert_to_df(self, self.rep_ref)

    @classmethod
    def _get_data(cls, self, method):
        """Get training data."""
        if method == "training":
            url = f"{self.baseUrl}/subset"
        elif method == "ref":
            url = f"{self.baseUrl}/subset_ref"

        get = requests.get(url)
        get.raise_for_status()
        return get

    @classmethod
    def _convert_to_df(cls, self, rep):
        """Convert to df, return dataframe."""
        df = pd.read_parquet(BytesIO(rep.content))
        return df


# %% codecell
