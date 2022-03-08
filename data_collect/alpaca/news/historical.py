"""Method for getting historical news articles."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import write_to_parquet, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import write_to_parquet, help_print_arg

# %% codecell


class ApcaNewsHistorical():
    """Get historical news articles, 50 at a time."""

    def __init__(self, rep, fpath, **kwargs):
        self.fpath = fpath
        rep_json = self._rep_to_json(self, rep, **kwargs)
        self.next_page_token = self._get_next_page_token(self, rep_json, **kwargs)
        self.df = self._convert_clean_to_df(self, rep_json, **kwargs)
        self._write_to_parquet(self)

    @classmethod
    def _rep_to_json(cls, self, rep, **kwargs):
        """Convert response object to json."""
        return rep.json()

    @classmethod
    def _get_next_page_token(cls, self, rep_json, **kwargs):
        """Extract next_page_token from call, if it exists."""
        return rep_json.get('next_page_token', None)

    @classmethod
    def _convert_clean_to_df(cls, self, rep_json, **kwargs):
        """Convert json to dataframe. Clean and return dataframe."""
        df_news = None
        try:
            df_news = pd.json_normalize(rep_json['news'])
        except KeyError:
            help_print_arg(rep_json.keys())
        df_news['created_at'] = (pd.to_datetime(df_news['created_at'])
                                   .dt.tz_convert('US/Eastern'))
        df_news['updated_at'] = (pd.to_datetime(df_news['updated_at'])
                                   .dt.tz_convert('US/Eastern'))
        return df_news

    @classmethod
    def _write_to_parquet(cls, self, **kwargs):
        """Write to parquet file."""
        kwargs = {'cols_to_drop': 'id'}
        write_to_parquet(self.df, self.fpath, combine=True, **kwargs)

# %% codecell
