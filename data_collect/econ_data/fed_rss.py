"""Federal reserve speeches."""
# %% codecell
from datetime import timedelta

from pathlib import Path
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import getDate, write_to_parquet, help_print_arg, baseDir
except ModuleNotFoundError:
    from multiuse.help_class import getDate, write_to_parquet, help_print_arg, baseDir

# %% codecell


class FedPressRss():
    """Federal reserve speeches rss feed."""

    def __init__(self, method, **kwargs):
        self._fpr_class_vars(self, method, **kwargs)
        self.df_speech = self._fpr_get_clean_data(self)
        self.upcoming_speech = self._fpr_get_relevant_dt(self, self.df_speech)

    @classmethod
    def _fpr_class_vars(cls, self, method, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')
        self.method = method

        # Get all the meetings today - in the next 2 business days
        self.dt = getDate.tz_aware_dt_now()
        self.dt_min = self.dt - timedelta(days=10)
        self.dt_max = self.dt + timedelta(days=30)

        # Get urls
        burl = 'https://www.federalreserve.gov/feeds'
        udict = ({
            'fed_speeches': f'{burl}/speeches.xml',
            'fed_press': f'{burl}/press_all.xml'
        })
        self.url = udict.get(method, 'No method selected for url')

        bpath = Path(baseDir().path, 'economic_data', 'FED')
        fdict = ({
            'fed_speeches': bpath.joinpath('fed_speeches.parquet'),
            'fed_press': bpath.joinpath('fed_press.parquet')
        })
        self.fpath = fdict.get(method, 'No method selected for fpath')

        if self.verbose:
            help_print_arg(f"FedPressRss: {self.method} {str(self.fpath)}")

        # Combine with other dictionary of fpaths, if exists
        if getattr(self, 'fdict', False):
            self.fdict = self.fdict | fdict
        else:
            self.fdict = fdict

    @classmethod
    def _fpr_get_clean_data(cls, self):
        """Get xml speech data from the federal reserve website."""
        df_xml = pd.read_xml(self.url, xpath='./channel/item')

        df_dt = (df_xml['pubDate'].str
                 .split(' ', expand=True)
                 .replace(',', '', regex=True))
        # Zero pad day of the month column
        df_dt[1] = df_dt[1].str.zfill(2)
        s_dt = df_dt.agg(''.join, axis=1).rename('pubDate')
        # Convert pubDate to pubTs - datetime timestamp
        df_xml['pubTs'] = pd.to_datetime(s_dt, format='%a%d%b%Y%H:%M:%S%Z')

        return df_xml

    @classmethod
    def _fpr_get_relevant_dt(cls, self, df_xml):
        """Get any row with recent or upcoming speeches."""
        dt_mask = ((df_xml['pubTs'] > self.dt_min) &
                   (df_xml['pubTs'] < self.dt))
        df_sub = df_xml[dt_mask]

        upcoming_speech = False
        if not df_sub.empty:  # Speeches soon/recently
            upcoming_speech = True

        return upcoming_speech

    @classmethod
    def _fpr_write_to_parquet(cls, self):
        """Combine with file if it exists on pubTs."""
        kwargs = {'cols_to_drop': ['pubTs']}
        write_to_parquet(self.speeches, self.fpath, **kwargs)
