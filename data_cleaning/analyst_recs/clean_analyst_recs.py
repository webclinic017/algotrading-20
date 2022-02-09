"""Clean analyst recs."""
# %% codecell
import numpy as np
import pandas as pd

try:
    from scripts.dev.api import serverAPI
    from scripts.dev.multiuse.pd_funcs import mask, perc_change
except ModuleNotFoundError:
    from api import serverAPI
    from multiuse.pd_funcs import mask, perc_change

pd.DataFrame.perc_change = perc_change
pd.DataFrame.mask = mask
# %% codecell


class CleanAnalystRatings():
    """Clean analyst price target ratings."""

    def __init__(self, df=None):
        if not isinstance(df, pd.DataFrame):
            self.df = self._get_df_from_api(self)
        else:
            self.df = df.copy()

        self._clean_missing_pt_info(self, self.df)
        self._convert_pt_rows(self, self.df)
        self._add_morning_night(self, self.df)

    @classmethod
    def _get_df_from_api(cls, self):
        """Get analyst recs df from server."""
        recs_all = serverAPI('analyst_recs_all').df
        cols_to_keep = (['ticker', 'date', 'time', 'action_company',
                         'action_pt', 'analyst', 'analyst_name',
                         'pt_prior', 'pt_current',
                         'rating_current', 'rating_prior', 'updated'])

        df = recs_all[cols_to_keep].reset_index(drop=True).copy()
        return df

    @classmethod
    def _clean_missing_pt_info(cls, self, df):
        """Clean missing action_pt data (10% roughly)."""
        pt_list = ['Raises', 'Announces', 'Lowers', 'Maintains', 'Adjusts']
        pt_none = df[~df['action_pt'].isin(pt_list)]

        pt_pos = pt_none.mask('action_company', 'Upgrades').index
        pt_neut = pt_none.mask('action_company', 'Initiates Coverage On').index
        pt_neg = pt_none.mask('action_company', 'Downgrades').index

        df.loc[pt_pos, 'action_pt'] = 'Raises'
        df.loc[pt_neut, 'action_pt'] = 'Announces'
        df.loc[pt_neg, 'action_pt'] = 'Lowers'

        df = df.copy()

    @classmethod
    def _convert_pt_rows(cls, self, df):
        """Swap '' with np.NaNs in price target columns."""
        cols_to_convert = ['pt_prior', 'pt_current']

        for col in cols_to_convert:
            pt_nan_idx = df[df[col] == ''].index
            df.loc[pt_nan_idx, col] = np.NaN

        df = df.copy()
        df[cols_to_convert] = (df[cols_to_convert].astype('object')
                                                  .astype(np.float64))

        df['pt_perc_change'] = df.perc_change('pt_prior', 'pt_current')

        self.df = df.copy()

    @classmethod
    def _add_morning_night(cls, self, df):
        """Add morning and night columns."""
        df['time'] = pd.to_datetime(df['time'])

        df['morning'] = np.where(
            df['time'].dt.hour < 12, 1, 0
        )

        df['night'] = np.where(
            df['time'].dt.hour > 12, 1, 0
        )

        self.df = df.copy()


# %% codecell
