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

    def __init__(self):
        self.get_params(self)
        self.get_rss_feed(self)
        self.clean_data(self)
        self.write_to_parquet(self)

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
        prev_15 = (datetime.now() - timedelta(minutes=11)).time()
        sec_df = (df[(df['dt'].dt.time > prev_15)
                  & (df['dt'].dt.date == date.today())]
                  .copy())

        self.df = df.copy()
        try:
            AnalyzeSecRss(latest=True, sec_df=sec_df)
        except Exception as e:
            help_print_arg(f"SecRss: AnalyzeSecRss Error {str(e)}")

    @classmethod
    def write_to_parquet(cls, self):
        """Read existing if exists - and/or write."""
        dt = getDate.query('sec_rss')

        f_suf = f"_{dt}.parquet"
        path = Path(baseDir().path, 'sec/rss', str(dt.year), f_suf)

        if path.exists():
            df_old = pd.read_parquet(path)
            df_all = pd.concat([df_old, self.df]).reset_index(drop=True)
            df_all.drop_duplicates(inplace=True)
            df_all.to_parquet(path)
        else:
            self.df.to_parquet(path)


# %% codecell


class AnalyzeSecRss():
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
            elif self.testing:
                help_print_arg("AnalyzeSecRss: testing msg send func")
                help_print_arg(str(msg))


# %% codecell
