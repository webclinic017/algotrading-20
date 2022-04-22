"""Intraday clean part 2."""
# %% codecell

import pandas as pd


try:
    from scripts.dev.multiuse.help_class import help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg, write_to_parquet


# %% codecell


class CboeIntraCleanP2():
    """Clean cboe intra clean p2."""

    def __init__(self, **kwargs):
        self._cicp2_get_class_vars(self, **kwargs)
        self.df_cb1 = self._cicp2_filter_construct_id(self)
        self.df_cb3 = self._cicp2_subset_and_prems(self, self.df_cb1)
        self._cicp2_write_to_parq(self, **kwargs)

    @classmethod
    def _cicp2_get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')

        self.f_cboe_intra = kwargs.get('f_cboe_intra')
        self.f_cboe3_clean = kwargs.get('f_cboe3_clean')

        if self.verbose:
            help_print_arg(f"CboeIntraCleanP2: f_cboe_intra: {str(self.f_cboe_intra)}")
            help_print_arg(f"CboeIntraCleanP2: f_cboe3_clean: {str(self.f_cboe3_clean)}")

    @classmethod
    def _cicp2_filter_construct_id(cls, self):
        """Filter and construct id from columns."""
        df_cb = pd.read_parquet(self.f_cboe_intra)
        df_cb = df_cb[df_cb['Volume'] > 1].reset_index(drop=True).copy()

        cols = df_cb.columns
        vc_dict = {}
        for col in cols:
            vc = df_cb[col].value_counts()
            if len(vc) < 5:
                if self.verbose:
                    print(col)
                vc_dict[col] = vc

        cols_to_drop = cols[cols.str.contains('Bid|Ask')]
        df_cb = df_cb.drop(columns=cols_to_drop).copy()
        df_cb = df_cb[df_cb['time'].dt.hour > 9].copy()

        times = df_cb['time'].value_counts()
        s_t_idx = pd.Series(*pd.factorize(times.index.sort_values()))

        df_cb['timeInt'] = df_cb['time'].map(s_t_idx)

        # %% codecell

        df_cb['expFmt'] = df_cb['Expiration'].dt.strftime('%Y%m%d')
        cols = (['Symbol', 'Call/Put', 'expFmt',
                 'Strike Price', 'Volume', 'Last Price', 'exch'])
        df_cb_0s = df_cb[cols].astype('str')

        df_cb_0s['id'] = (df_cb_0s['Symbol'] + '_' + df_cb_0s['Call/Put']
                          + '_' + df_cb_0s['expFmt']
                          + '_' + df_cb_0s['Strike Price']
                          + '_' + df_cb_0s['Volume']
                          + '_' + df_cb_0s['Last Price']
                          + '_' + df_cb_0s['exch'])

        df_cb_0s['id2'] = (df_cb_0s['id'].str.strip()
                           .str.replace('\s', '', regex=True))
        df_cb['id2'] = df_cb_0s['id2']

        return df_cb

    @classmethod
    def _cicp2_subset_and_prems(cls, self, df_cb1):
        """Get the subset and premiums."""
        df_cb1 = df_cb1.set_index('id2')
        df_cb2 = df_cb1[~df_cb1.index.duplicated()].copy()

        df_cb3 = df_cb2[df_cb2['time'].dt.hour > 9].copy()
        df_cb3['prem'] = (df_cb3['Volume'].mul(df_cb3['Last Price'])
                          .astype('int') * 100)
        df_cb3['prem/mil'] = df_cb3['prem'].div(1000000)
        # Trades over 10k
        df_cb3 = df_cb3[df_cb3['prem/mil'] > .01].copy()

        cols_to_drop = ['expFmt', 'prem']
        df_cb3 = df_cb3.drop(columns=cols_to_drop)
        df_cb3['Last Price'] = df_cb3['Last Price'].astype('float64').round(2)
        str_cols = df_cb3.select_dtypes(include=['category', 'object']).columns
        df_cb3[str_cols] = df_cb3[str_cols].astype('str').astype('category')

        return df_cb3

    @classmethod
    def _cicp2_write_to_parq(cls, self, **kwargs):
        """Write cleaned cboe intraday to dataframe."""
        write_to_parquet(self.df_cb3, self.f_cboe3_clean)
        if self.verbose:
            help_print_arg("CboeIntraCleanP2: Finished writing to parq")
