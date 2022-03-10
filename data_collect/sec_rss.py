"""Studying SEC RSS Feeds."""
# %% codecell
from pathlib import Path
from datetime import datetime, timedelta, date
import requests
import pandas as pd

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
        self.clean_data(self)
        # self._write_to_parquet(self)

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

        self.df = df.copy()
        if self.testing:
            AnalyzeSecRss(latest=True, df=self.df)
        else:
            try:
                AnalyzeSecRss(latest=True, df=self.df)
            except Exception as e:
                help_print_arg(f"SecRss: AnalyzeSecRss Error {str(e)}")

    @classmethod
    def _write_to_parquet(cls, self):
        """Read existing if exists - and/or write."""
        self.df['pubDate'] = self.df['pubDate'].dt.to_pydatetime()
        kwargs = {'cols_to_drop': 'guid'}
        write_to_parquet(self.df, self.fpath, combine=True, **kwargs)


# %% codecell


class AnalyzeSecRss():
    """Analyze sec rss latest 200 symbols."""

    def __init__(self, df=None, **kwargs):
        self._get_class_vars(self, **kwargs)
        self.df = self._get_sec_df(self, df, **kwargs)
        self.df_clean = self._get_merge_ref_data(self, self.df, **kwargs)
        self.my_sec = self._get_df_today_my_symbols(self, **kwargs)
        self.df_msgs = self._get_pos_msgs_today(self, **kwargs)
        self.df_new = self._confirm_msg_to_send(self, self.df_msgs, **kwargs)
        # Only fire off new messages not already sent
        if isinstance(self.df_new, pd.DataFrame):
            self.df_new_sent = self._iterate_send_msgs(self, **kwargs)

    @classmethod
    def _get_class_vars(cls, self, **kwargs):
        """Get class variables."""
        bdir = Path(baseDir().path, 'telegram', 'sec', 'rss')
        self.fpath = bdir.joinpath('_sec_rss_sent.parquet')

        self.dt = kwargs.get('dt', getDate.query('iex_close'))
        # If we want to skip writing locally
        self.skip_write = kwargs.get('skip_write', False)
        # If we want to print
        self.verbose = kwargs.get('verbose', False)

    @classmethod
    def _get_sec_df(cls, self, df, **kwargs):
        """Check if sec_df is dataframe, if not, get it."""
        if not isinstance(df, pd.DataFrame):
            df = serverAPI('sec_rss_latest').df

        if 'CIK' in df.columns:
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
    def _get_df_today_my_symbols(cls, self, **kwargs):
        """Reduce to my symbols, to only most recent rss list."""
        my_symbols = kwargs.get('my_symbols', False)
        if not my_symbols:
            my_stocks = ['CVS', 'MKTW']

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
        cols = ['symbol', 'dt', 'cik', 'msg', 'form']

        if not self.my_sec.empty:
            for index, row in self.my_sec.iterrows():
                if row['cik']:
                    msg1 = f"{row['symbol']} filed form {row['description']} "
                    msg = f"{msg1} at {str(row['dt'])}"
                    (msg_list.append((row['symbol'], row['dt'],
                                      row['cik'], msg, row['description'])))

        df_msgs = pd.DataFrame(msg_list, columns=cols)
        return df_msgs

    @classmethod
    def _confirm_msg_to_send(cls, self, df_msgs, **kwargs):
        """Check for file of sent messages else send all."""
        cols_merge = ['cik', 'dt', 'msg']
        if self.fpath.exists():
            df_msgs_all = pd.read_parquet(self.fpath)
            # Get only the sent messages from today
            df_msgs_all[df_msgs_all['dt'].dt.date == self.dt]
            # Merge to get the messages already sent, for today
            df_comb = (df_msgs_all.merge(df_msgs[cols_merge],
                                         on=cols_merge, indicator=True))
            # Return all msgs not in msg sent df
            if not df_comb.empty:
                new_idx = (df_comb['indicator'] == 'right_only').index
                df_new_msgs = (df_msgs[
                               df_msgs.index.intersection(new_idx)].copy())
                return df_new_msgs
            # Nothing to sort through here - nothing in common.
            elif df_comb.empty:
                return None
        else:  # So we can send all messages since none have been sent
            return self.df_msgs

    @classmethod
    def _iterate_send_msgs(cls, self, **kwargs):
        """Send each message to telegram."""
        # It's assumed that all messages are ready to send at this point
        # No duplicates exist, nothing has already been sent
        tmsg_list = []
        for index, row in self.df_new.iterrows():
            tmsg = telegram_push_message(row['msg'], sec_forms=True)
            tmsg_list.append(tmsg.json()['result'])

        # Index should be fine
        df_new_sent = self.df_new.join(pd.json_normalize(tmsg_list))
        cols_to_drop = (['text', 'chat.type',
                         'chat.all_members_are_administrators'])
        df_new_sent.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        cols_to_drop = ['cik', 'dt']
        if not self.skip_write:
            (write_to_parquet(df_new_sent, self.fpath, combine=True,
                              cols_to_drop=cols_to_drop, **kwargs))

        return df_new_sent


# %% codecell


class SecAnalyzeRss():  # Deprecated
    """Analyze sec rss feed for stocks already invested."""

    def __init__(self, latest=True, sec_df=None, testing=False):
        self.testing, self.sec_df = testing, sec_df

        if isinstance(sec_df, pd.DataFrame):
            # latest = False
            if 'symbol' in sec_df.columns:
                self.df = sec_df
                if testing:
                    help_print_arg('Symbol in sec_df.columns')
            else:
                self.get_merge_ref_data(self)
        else:  # If sec_df is not a dataframe (False)
            self.retrieve_df(self, latest)
            self.get_merge_ref_data(self)
            if testing:
                help_print_arg('Retrieving df')
                help_print_arg('get_merge_ref_data')

        self.filter_my_stocks(self)
        self.send_text_messages(self)

    @classmethod
    def retrieve_df(cls, self, latest):
        """Retrieve latest sec df if no df is passed."""
        sec_df = serverAPI('sec_rss_latest').df
        # Rename columns, drop duplicates, and reset index
        sec_df = (sec_df.rename(columns={'CIK': 'cik', 'description': 'form'})
                        .drop_duplicates(subset=['cik', 'pubDate'])
                        .reset_index(drop=True))
        sec_df['dt'] = pd.to_datetime(sec_df['pubDate'])

        if latest:  # Get data from latest rss (10 minutes)
            prev_15 = (datetime.now() - timedelta(minutes=15)).time()
            sec_df = (sec_df[(sec_df['dt'].dt.time > prev_15)
                      & (sec_df['dt'].dt.date == date.today())]
                      .copy())
        # Store under class variable
        self.sec_df = sec_df.copy()

    @classmethod
    def get_merge_ref_data(cls, self):
        """Get reference data for sec_df."""
        sym_refs = get_all_symbol_ref()
        type_list = ['cs', 'ad', 'et']
        cs_adr = (sym_refs[sym_refs['type'].isin(type_list)]
                                           .copy()
                                           .drop_duplicates(subset=['cik'])
                                           .reset_index(drop=True))

        if ('form' or 'cik') not in self.sec_df.columns:
            col_dict = {'description': 'form', 'CIK': 'cik'}
            self.sec_df.rename(columns=col_dict, inplace=True)

        # self.sec_df['cik'] = self.sec_df['cik'].astype('float64')
        # cs_adr['cik'] = cs_adr['cik'].astype('float64')
        # Merge reference data with sec_df
        df = pd.merge(self.sec_df, cs_adr, on=['cik'], how='left', validate='m:1')
        self.df = df.copy()

    @classmethod
    def filter_my_stocks(cls, self):
        """Filter dataframe for my stocks."""
        path = Path(baseDir().path, 'tickers', 'my_syms.parquet')
        my_df = pd.read_parquet(path)
        # Convert local dataframe to syms to look for
        inv_list = my_df['symbol'].tolist()

        if ('form' or 'cik') not in self.df.columns:
            col_dict = {'description': 'form', 'CIK': 'cik'}
            self.df.rename(columns=col_dict, inplace=True)

        df_inv = self.df[self.df['symbol'].isin(inv_list)].copy()

        if (df_inv.shape[0] == 0) and self.testing:
            help_print_arg("AnalyzeSecRss: no matching stocks for rss feed")

        # forms_to_watch = ['8-K', '3', '4']
        # df_forms = df_inv[df_inv['form'].isin(forms_to_watch)]

        msg_dict = {sym: [] for sym in inv_list}
        for index, row in df_inv.iterrows():
            if row['cik']:
                msg = f"{row['symbol']} has just filed form {row['form']}"
                msg_dict[row['symbol']].append(msg)

        self.msg_dict = msg_dict
        self.df_inv = df_inv.copy()

    @classmethod
    def send_text_messages(cls, self):
        """Send text messages to myself with relevant data."""
        for key, msg in self.msg_dict.items():
            if msg:
                # send_twilio_message(msg=msg)
                telegram_push_message(text=msg, sec_forms=True)
                help_print_arg(f"Sec RSS send message: {str(msg)}")
            elif self.testing:
                help_print_arg("AnalyzeSecRss: testing msg send func")
                help_print_arg(str(msg))


# %% codecell
