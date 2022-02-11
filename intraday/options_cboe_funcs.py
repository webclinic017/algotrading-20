"""Class helper function for intraday cboe options."""
# %% codecell
from datetime import timedelta

import pandas as pd

try:
    from scripts.dev.api.serverAPI import serverAPI
except ModuleNotFoundError:
    from api import serverAPI

# %% codecell


class IntradayOptionsCBOEHelpers():
    """Helper functions for cboe intraday options."""

    """
    var: self.intra_df : intraday cboe df
    var: self.ykeep : subset of yoptions to merge
    var: self.df : merged cboe intraday and yoptions
    var: self.continue : to continue with class functions
    """

    def __init__(self):
        self._get_clean_intraday_data(self)
        self._intraday_yoptions_clean(self)
        self._merge_store_dataframe(self)

    @classmethod
    def _get_clean_intraday_data(cls, self):
        """Get latest cboe intraday data, clean for functions."""
        intra_df = serverAPI('cboe_intraday_eod').df

        cols = [col.lower() for col in intra_df.columns]
        intra_df.columns = cols

        intra_df = self._build_contract_symbol_col(self, intra_df)

        intra_df['premium'] = intra_df['volume'].mul(intra_df['last price'])
        cols_to_sum = ['contractSymbol', 'volume', 'premium', 'routed']
        summed_cs_vol = (intra_df[cols_to_sum]
                         .groupby(by=['contractSymbol'], as_index=False)
                         .sum())

        cols_to_drop = ['volume', 'premium', 'routed', 'exch']
        intra_dropped = intra_df.drop(columns=cols_to_drop)
        intra_dropped.drop_duplicates(subset=['contractSymbol'], inplace=True)

        intra_df = (pd.merge(intra_dropped, summed_cs_vol,
                             on=['contractSymbol']))

        self.intra_df = intra_df

    @classmethod
    def _build_contract_symbol_col(cls, self, df):
        """Build cboe intraday contract symbol column."""

        df['strike price'] = df['strike price'].astype('float64').round(1)

        df['sp_suf'] = (df.apply(
                              lambda row: f"{row['strike price']}00",
                              axis=1))
        df['contractSymbol'] = (df.apply(lambda row: f"""{row['symbol']}
                                      {row['expiration'].strftime('%y%m%d')}
                                      {row['call/put']}
                                      {row['sp_suf'].zfill(9)}""", axis=1))
        df['contractSymbol'] = (df['contractSymbol'].str
                                .replace('.', '', regex=False)
                                .replace('\n ', '', regex=True)
                                .replace(' ', '', regex=True))

        df.drop(columns=['sp_suf'], inplace=True)

        return df

    @classmethod
    def _intraday_yoptions_clean(cls, self):
        """Get and clean yoptions data for OI merging."""
        yoptions = serverAPI('yoptions_all').df

        yopt_include = ['contractSymbol', 'openInterest', 'lastTradeDate']
        ytest = (yoptions[yopt_include].sort_values(
                 by=['lastTradeDate'], ascending=False)
                 .drop_duplicates(subset=['contractSymbol'], keep='first'))

        ykeep = (ytest[ytest['lastTradeDate'] >
                 (ytest['lastTradeDate'].max() - timedelta(days=20))]
                 .drop(columns=['lastTradeDate']))

        self.ykeep = ykeep

    @classmethod
    def _merge_store_dataframe(cls, self):
        """Merge cboe intraday with yoptions."""
        df = (pd.merge(self.intra_df, self.ykeep,
              on=['contractSymbol'], how='left'))
        df['vol/oi'] = df['volume'].div(df['openInterest'])

        self.df = df


# %% codecell
