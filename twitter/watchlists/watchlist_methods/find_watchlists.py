"""Twitter watchlists."""
# %% codecell

import numpy as np
import pandas as pd

try:
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from api import serverAPI

# %% codecell


class TwitterWatchlists():
    """Get twitter watchlists from historical data."""

    def __init__(self, df=None, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        df = self._get_dataframe(self, df, **kwargs)
        self.df_tw = self._exclude_sym_hash_cols(self, df, **kwargs)
        self.df_watch = self._isolate_watchlists(self, self.df_tw, **kwargs)
        self.df_allW = self._create_df_splits(self, self.df_watch, **kwargs)
        if self.verbose:
            print('Final df accessible under TwitterWatchlists.df_allW')

    @classmethod
    def _get_dataframe(cls, self, df, **kwargs):
        """Determine if dataframe passed or get if needed."""
        if not isinstance(df, pd.DataFrame):
            df = serverAPI('twitter_hist_all').df
        return df

    @classmethod
    def _exclude_sym_hash_cols(cls, self, df, **kwargs):
        """Exclude sym, hash, entity columns."""
        reg_exclude = 'entities.|sym_|hash_'
        cols_to_exclude = (df.columns[df.columns.str
                                        .contains(reg_exclude, regex=True)])
        df = (df.drop(columns=cols_to_exclude)
                .reset_index(drop=True).copy())
        return df

    @classmethod
    def _isolate_watchlists(cls, self, df, **kwargs):
        """Use regex to isolate rows with watch or list in text."""
        df['syms'] = (df['text'].str
                      .findall(r'\$([A-Z]+)')
                      .apply(lambda x: len(list(set(x)))))
        df['call#'] = (df['text']
                       .str.findall('( [0-9]+c )')
                       .apply(lambda x: len(x)))
        df['put#'] = (df['text']
                      .str.findall('( [0-9]+p )')
                      .apply(lambda x: len(x)))
        df['%Count'] = (df['text'].str.findall('%|➡️')
                        .apply(lambda x: len(x))
                        .replace(0, np.NaN))

        df['%/syms'] = (df['syms'].div(df['%Count'])
                        .round(2).dropna())

        cond_3 = (df['syms'] >= 3)
        cond_RT = (~df['RT'])
        cond_few_percs = ((df['%/syms'] > 2) | (df['%/syms'].isna()))
        cond_dervs = ((df['call#'] > 1) & (df['put#'] > 1))

        df_watch = (df[cond_3 & cond_RT & cond_few_percs
                       & cond_dervs].copy())

        return df_watch

    @classmethod
    def _create_df_splits(cls, self, df_watch, **kwargs):
        """Create dataframe of splits, strikes, prices."""
        watch_rows = (df_watch['text']
                      .str.extractall(r'(\$[A-Z]+.+)')
                      .dropna()[0]
                      .str.split('|', expand=True))

        call_splits = (watch_rows[0].str.strip()
                                    .str.split(' +', expand=True)
                                    .drop(columns=[2])
                                    .rename(columns={0: 'symbol',
                                                     1: 'callS',
                                                     3: 'callP'}))
        put_splits = (watch_rows[1].str.strip()
                                   .str.split(' +', expand=True)
                                   .drop(columns=[1])
                                   .rename(columns={0: 'putS',
                                                    2: 'putP'}))

        df_allW = (call_splits.join(put_splits)
                              .reset_index(level=1, drop=True)
                              .reset_index()
                              .rename(columns={'index': 'ogIdx'}))

        df_allW['symbol'] = df_allW['symbol'].str.replace('\$', '', regex=True)
        col_order = ['symbol', 'callP', 'callS', 'putP', 'putS', 'ogIdx']
        df_allW = df_allW[col_order]

        return df_allW
