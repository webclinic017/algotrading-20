"""Studying SEC RSS Feeds."""
# %% codecell
from pathlib import Path
import requests
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg
    from scripts.dev.multiuse.create_file_struct import make_yearly_dir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, help_print_arg
    from multiuse.create_file_struct import make_yearly_dir
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
        headers = ({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0',
                    'Referer': 'https://www.sec.gov/structureddata/rss-feeds-submitted-filings',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cache-Control': 'no-cache',
                    'Accept-Language': 'en-GB,en;q=0.5'})
        self.url = url
        self.headers= headers

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

        self.df = df.copy()

    @classmethod
    def write_to_parquet(cls, self):
        """Read existing if exists - and/or write."""
        dt = getDate.query('iex_close')

        f_suf = f"_{dt}.parquet"
        path = Path(baseDir().path, 'sec/rss', str(dt.year), f_suf)

        if path.exists():
            df_old = pd.read_parquet(path)
            df_all = pd.concat([df_old, self.df]).reset_index(drop=True)
            df_all.to_parquet(path)
        else:
            self.df.to_parquet(path)



# %% codecell


# %% codecell


# %% codecell

# %% codecell
