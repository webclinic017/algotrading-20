"""Historical news loop batch call."""
# %% codecell
from datetime import datetime, timedelta
import time

from tqdm import tqdm
import pandas as pd

try:
    from scripts.dev.data_collect.alpaca.api_calls.apca_api import ApcaAPI
    from scripts.dev.data_collect.alpaca.methods.apca_paths import ApcaPaths
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from data_collect.alpaca.api_calls.apca_api import ApcaAPI
    from data_collect.alpaca.methods.apca_paths import ApcaPaths
    from multiuse.help_class import help_print_arg

# %% codecell


class HistoricalNewsLoop():
    """Loop for getting historical news articles."""

    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.news_base = self._get_news_base(self, **kwargs)
        next_page_token = self.news_base.next_page_token
        self._start_historical_loop(self, next_page_token, **kwargs)

    @classmethod
    def _get_news_base(cls, self, **kwargs):
        """Get the base news object whether df exists or not."""
        ap = ApcaPaths(api_val='news_historical', **{'return_df': True})
        if isinstance(ap.df, pd.DataFrame):
            kwargs = {'params': {'end': ap.df['created_at'].min().isoformat()}}

        news_base = ApcaAPI(api_val='news_historical', **kwargs)
        return news_base

    @classmethod
    def _start_historical_loop(cls, self, next_page_token, **kwargs):
        """Start the historical news loop."""
        next_min = datetime.now() + timedelta(seconds=60)
        count = 0

        for n in tqdm(range(365)):
            if datetime.now() >= next_min:
                if self.verbose:
                    help_print_arg("Historical news loop: resetting minute")
                next_min = datetime.now() + timedelta(seconds=60)
                count = 0
            elif datetime.now() <= next_min and count >= 190:
                if self.verbose:
                    help_print_arg("Historical news loop: hit 200 before next_min")
                seconds_remaining = (next_min - datetime.now()).seconds
                time.sleep(seconds_remaining)
                next_min = datetime.now() + timedelta(seconds=60)
                count = 0

            kwargs = {'params': {'page_token': next_page_token}}
            aa = ApcaAPI(api_val='news_historical', **kwargs)
            next_page_token = aa.next_page_token
            count += 1


# %% codecell
