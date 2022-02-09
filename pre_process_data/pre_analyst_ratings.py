"""Preprocess Analyst Ratings."""
# %% codecell

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import help_print_arg
    from scripts.dev.multiuse.pd_funcs import vc
    from scripts.dev.multiuse.df_helpers import DfHelpers
    from scripts.dev.data_cleaning.analyst_recs.clean_analyst_recs import CleanAnalystRatings
    from scripts.dev.data_cleaning.analyst_recs.ratings_current_dist import RatingCurrentDist
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg
    from multiuse.pd_funcs import vc
    from multiuse.df_helpers import DfHelpers
    from data_cleaning.analyst_recs.clean_analyst_recs import CleanAnalystRatings
    from data_cleaning.analyst_recs.ratings_current_dist import RatingCurrentDist

 # %% codecell

pd.DataFrame.vc, pd.Series.vc = vc, vc

 # %% codecell


class PreProcessAnalystRatings():
    """Pre-process analyst ratings."""

    df = None

    def __init__(self, df=None, df_all=None):
        car = CleanAnalystRatings()
        rcd = RatingCurrentDist(car.df, preprocess=True)
        df = self._remove_rare_acs(self, rcd.df)
        df = DfHelpers.remove_cats_no_counts(df)
        df = self._convert_ac_col(self, df, 'action_company')
        df = self._convert_ac_col(self, df, 'action_pt')
        df = self._drop_columns(self, df)
        df = self._groupby_merge(self, df)

        if isinstance(df_all, pd.DataFrame):
            df = self._combine_with_cleaned(self, df_all, df)

        self.df = df.copy()

    @classmethod
    def _remove_rare_acs(cls, self, df):
        """Remove non common action company rows."""
        # Remove rows not in the Raises, Announces, Lowers
        pt_list = ['Raises', 'Announces', 'Lowers']
        df = df[df['action_pt'].isin(pt_list)].copy()
        ac_list = (['Maintains', 'Initiates Coverage On',
                    'Upgrades', 'Downgrades'])
        df = df[df['action_company'].isin(ac_list)].copy()
        return df

    @classmethod
    def _convert_ac_col(cls, self, df, ac_col):
        """Convert action company column to one/hot encoding."""
        ac_col_dict = {}

        if ac_col == 'action_company':
            # Cols start with acc
            ac_vc = df[ac_col].vc()
            cols = ac_vc.index.tolist()
            ac_col_dict = {col: f"acc_{col[:4].lower()}" for col in cols}
        elif ac_col == 'action_pt':
            ac_col_dict = ({'Raises': 'acp_upgr', 'Announces': 'acp_init',
                            'Lowers': 'acp_down'})
        else:
            msg = "_convert_ac_col: ac_col not recognized"
            help_print_arg(msg)
            return

        df = df.copy()
        for key, val in ac_col_dict.items():
            df[val] = np.where(
                df[ac_col] == key, 1, 0
            )
        return df

    @classmethod
    def _drop_columns(cls, self, df):
        """Drop columns before preprocessing."""
        cols_to_remove = (['action_company', 'action_pt', 'analyst',
                           'analyst_name', 'rating_current',
                           'rating_prior', 'updated', 'time'])

        df.drop(columns=cols_to_remove, inplace=True, errors='ignore')
        df.rename(columns={'ticker': 'symbol'}, inplace=True)
        return df

    @classmethod
    def _groupby_merge(cls, self, df):
        """Groupby and merge dataframes."""
        cols = df.columns
        cols_to_mean = ['pt_prior', 'pt_current', 'pt_perc_change']
        cols_to_sum = cols.difference(cols_to_mean)

        gp_sum = df[cols_to_sum].groupby(by=['symbol', 'date']).sum()
        gp_sums = gp_sum[gp_sum.any(axis=1) != 0].reset_index()

        gp_mean = (df[cols_to_mean + ['symbol', 'date']]
                   .groupby(by=['symbol', 'date'])
                   .mean().dropna().reset_index())

        df_joined = (pd.merge(gp_sums, gp_mean,
                     on=['symbol', 'date'], how='left'))
        return df_joined

    @classmethod
    def _combine_with_cleaned(cls, self, df_all, df):
        """If df_all is passed, combine and return dataframe."""
        df_merged = pd.merge(df_all, df, on=['symbol', 'date'], how='left')
        return df_merged



 # %% codecell
