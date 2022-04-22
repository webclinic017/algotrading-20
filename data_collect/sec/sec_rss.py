"""Studying SEC RSS Feeds."""
# %% codecell
from pathlib import Path
from datetime import datetime, timedelta, date
import requests
import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg, write_to_parquet
    from scripts.dev.multiuse.symbol_ref_funcs import get_all_symbol_ref
    from scripts.dev.multiuse.create_file_struct import make_yearly_dir
    from scripts.dev.multiuse.comms import send_twilio_message
    from scripts.dev.telegram.methods.push import telegram_push_message
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg, write_to_parquet
    from multiuse.symbol_ref_funcs import get_all_symbol_ref
    from multiuse.create_file_struct import make_yearly_dir
    from multiuse.comms import send_twilio_message
    from telegram.methods.push import telegram_push_message
    from api import serverAPI


# %% codecell

# base_path = Path(baseDir().path, 'sec/rss')
# make_yearly_dir(base_path)
# url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"

# %% codecell


class SecRssFeed():
    """Collect rss feed from sec every 10 minutes."""
    "These feeds are updated every ten minutes Monday through Friday, 6am – 10pm EST"

    def __init__(self, **kwargs):
        self.testing = kwargs.get('testing', False)
        self.fpath = self._get_fpath(self)
        self.get_params(self)
        self.get_rss_feed(self)
        self.df = self.clean_data(self)
        self._write_to_parquet(self)
        self._call_analyze_rss(self, **kwargs)

    @classmethod
    def _get_fpath(cls, self):
        """Get fpath to use."""
        dt = getDate.query('sec_rss')
        f_suf = f"_{dt}.parquet"
        fpath = Path(baseDir().path, 'sec/rss', str(dt.year), f_suf)
        return fpath

    @classmethod
    def get_params(cls, self):
        """Prepare header dict."""
        url = "https://www.sec.gov/Archives/edgar/xbrlrss.all.xml"
        headers = ({'User-Agent': 'Rogue Technology Ventures edward@rogue.golf',
                    'Referer': 'https://www.sec.gov/structureddata/rss-feeds-submitted-filings',
                    'Host': 'www.sec.gov',
                    'Accept-Encoding': 'gzip, deflate',
                    'Cache-Control': 'no-cache',
                    'Accept-Language': 'en-GB,en;q=0.5'})
        self.url = url
        self.headers = headers

    @classmethod
    def get_rss_feed(cls, self):
        """Request and retry to get data from sec."""
        get = requests.get(self.url, headers=self.headers)
        if get.status_code >= 400:
            get = requests.get(self.url, headers=self.headers)
            if get.status_code >= 400:
                help_print_arg('SEC RSS Feed: 2nd get request failed')

        self.df = pd.read_xml(get.content, xpath='.//item')

    @classmethod
    def clean_data(cls, self):
        """Clean dataframe - remove na columns."""
        df = self.df
        na_cutoff = (.75 * df.shape[0])
        cols_to_drop = []
        for col in df.columns:
            if df[col].isna().sum() > na_cutoff:
                cols_to_drop.append(col)

        # Drop columns that are at least 3/4 nas
        df.drop(columns=cols_to_drop, inplace=True)

        # Extract CIK from title column
        try:
            df['CIK'] = df['title'].str.extract("\((.*?)\)")
        except Exception as e:
            help_print_arg(f"SEC RSS CIK Error: {str(e)}")

        df['dt'] = pd.to_datetime(df['pubDate'])
        dtmap = {'EST': 'US/Eastern', 'EDT': 'US/Central'}
        df['pubDate'] = (df['pubDate'].str.replace(',+', '', regex=True)
                                      .replace(dtmap, regex=True)
                                      .str.strip())
        pdFormat = '%a %d %b %Y %H:%M:%S %Z'
        df['pubDate'] = (pd.to_datetime(
                         df['pubDate'], format=pdFormat, utc=True))
        # Convert to timestamp
        df['dt'] = df['pubDate'].apply(lambda x: x.timestamp())
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
        return df

    @classmethod
    def _write_to_parquet(cls, self):
        """Read existing if exists - and/or write."""
        self.df['pubDate'] = self.df['pubDate'].dt.to_pydatetime()
        kwargs = {'cols_to_drop': 'guid'}
        write_to_parquet(self.df, self.fpath, combine=True, **kwargs)

    @classmethod
    def _call_analyze_rss(cls, self, **kwargs):
        """Call analyze sec rss, send to telegram if matching companies."""
        if self.testing:
            # AnalyzeSecRss(df=self.df, **kwargs)
            AnalyzeSecRss(**kwargs)
        else:
            try:
                AnalyzeSecRss(**kwargs)
            except Exception as e:
                help_print_arg(f"SecRss: AnalyzeSecRss Error {str(e)}")


# %% codecell


class AnalyzeSecRss():
    """Analyze sec rss latest 200 symbols."""
    # Gets the dataframe from latest day - no dataframe should be passed
    # Idemptotent transaction here

    def __init__(self, df=None, **kwargs):
        self._get_class_vars(self, **kwargs)
        self.df = self._get_sec_df(self, df, **kwargs)
        self.df_clean = self._get_merge_ref_data(self, self.df, **kwargs)
        self.my_sec = self._get_df_today_my_symbols(self, self.my_stocks, **kwargs)
        self.df_msgs = self._get_pos_msgs_today(self, **kwargs)
        self.df_new_msgs = self._confirm_msg_to_send(self, self.df_msgs, **kwargs)
        # Only fire off new messages not already sent
        if isinstance(self.df_new_msgs, pd.DataFrame):
            self.df_new_sent = self._iterate_send_msgs(self, self.df_new_msgs, **kwargs)

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Get class variables."""
        bdir = Path(baseDir().path, 'social', 'telegram', 'sec')
        self.fpath = bdir.joinpath('_sec_rss_sent.parquet')

        self.my_stocks = False
        # fpath for my symbols
        f_my_syms = Path(baseDir().path, 'tickers', 'my_syms.parquet')
        if f_my_syms.exists():
            self.my_stocks = pd.read_parquet(f_my_syms)['symbol'].tolist()
        # Get class date variable for most recent date
        self.dt = kwargs.get('dt', getDate.query('iex_close'))
        # If we want to skip writing locally
        self.skip_write = kwargs.get('skip_write', False)
        # If we want to print
        self.verbose = kwargs.get('verbose', False)
        # If we want to test only
        self.testing = kwargs.get('testing', False)
        # If we want to create symbol list from existing in sec_rss_latest
        self.create_symbols = kwargs.get('create_symbols', False)

    @classmethod
    def _get_sec_df(cls, self, df, **kwargs):
        """Check if sec_df is dataframe, if not, get it."""
        if not isinstance(df, pd.DataFrame):
            df = serverAPI('sec_rss_latest').df

        cols = df.columns
        if ('cik' in cols) and ('CIK' in cols):
            df['cik'] = (np.where(
                df['CIK'], df['CIK'], df['cik']
            ))
            df.drop(columns=['CIK'], inplace=True)
        elif ('CIK' in cols) and ('cik' not in cols):
            df.rename(columns={'CIK': 'cik'}, inplace=True)
        # if df['pubDate'] is not datetime
        return df

    @classmethod
    def _get_merge_ref_data(cls, self, df, **kwargs):
        """Get reference data."""
        # Make sure that the df passed isn't already combined
        if 'type' not in df.columns:
            sec_ref = serverAPI('sec_ref').df
            # Get all symbols (IEX ref)
            all_syms = serverAPI('all_symbols').df
            all_syms.drop_duplicates(subset='cik', inplace=True)
            df_ref = (sec_ref.merge(
                      all_syms[['symbol', 'cik', 'type']],  on='cik'))

            if self.verbose:
                print(str(df_ref.columns))
                print(str(df.columns))
            sec_all = df.merge(df_ref, on='cik')
            return sec_all
        else:  # It's not assumed that the sec data is from most recent day
            return df

    @classmethod
    def _get_df_today_my_symbols(cls, self, my_stocks, **kwargs):
        """Reduce to my symbols, to only most recent rss list."""
        if not my_stocks or self.create_symbols:
            my_stocks = self.df_clean['symbol'].value_counts().index[0:3]

        df_my_sec = (self.df_clean[self.df_clean['symbol']
                     .isin(my_stocks)].copy())

        my_sec_today = (df_my_sec[df_my_sec['dt'].dt.date == self.dt]
                        .drop_duplicates(subset=['link', 'description']
                        .copy()))

        if my_sec_today.empty:
            return pd.DataFrame()
        else:
            return my_sec_today

    @classmethod
    def _get_pos_msgs_today(cls, self, **kwargs):
        """Get possible messages today to send."""
        msg_list = []
        cols = ['symbol', 'pubDate', 'cik', 'msg', 'form', 'guid']

        if not self.my_sec.empty:
            for index, row in self.my_sec.iterrows():
                if row['cik']:
                    msg1 = f"{row['symbol']} filed form {row['description']} "
                    msg = f"{msg1} at {str(row['pubDate'])}"
                    (msg_list.append((row['symbol'], row['pubDate'], row['cik'],
                                      msg, row['description'], row['guid'])))

        df_msgs = pd.DataFrame(msg_list, columns=cols)
        return df_msgs

    @classmethod
    def _confirm_msg_to_send(cls, self, df_msgs, **kwargs):
        """Check for file of sent messages else send all."""
        if self.fpath.exists():
            df_all = pd.read_parquet(self.fpath)
            # Get only the sent messages from today
            df_all = df_all[df_all['pubDate'].dt.date == self.dt]
            df_new_msgs = (df_msgs[~df_msgs['guid']
                           .isin(df_all.get('guid', []))]
                           .copy())

            return df_new_msgs
        else:  # So we can send all messages since none have been sent
            if self.verbose:
                help_print_arg('SEC _confirm_msg_to_send: Local message path does not exist.')
            return self.df_msgs

    @classmethod
    def _iterate_send_msgs(cls, self, df_new_msgs, **kwargs):
        """Send each message to telegram."""
        # It's assumed that all messages are ready to send at this point
        # No duplicates exist, nothing has already been sent
        tmsg_list = []
        for index, row in df_new_msgs.iterrows():
            tmsg = telegram_push_message(row['msg'], sec_forms=True)
            tmsg_list.append(tmsg.json()['result'])

        # Index should be fine
        df_new_sent = df_new_msgs.join(pd.json_normalize(tmsg_list))
        cols_to_drop = (['text', 'chat.type',
                         'chat.all_members_are_administrators'])
        df_new_sent.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        if not self.skip_write:
            (write_to_parquet(df_new_sent, self.fpath, combine=True,
                              cols_to_drop=['guid'], **kwargs))

        return df_new_sent


# %% codecell
