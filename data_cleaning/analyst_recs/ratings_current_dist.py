"""Ratings Current distribution."""
# %% codecell

import numpy as np
import pandas as pd

try:
    from scripts.dev.multiuse.pd_funcs import mask, vc
except ModuleNotFoundError:
    from multiuse.pd_funcs import mask, vc

pd.DataFrame.vc, pd.Series.vc = vc, vc
pd.DataFrame.mask = mask

# %% codecell


class RatingCurrentDist():
    """Get pos, neg, and neutral columns + distributions."""

    word_dict = {}
    col_dict = {}

    def __init__(self, df, action_company=None, preprocess=False):
        self.df = df.copy()
        self._make_word_dict(self, self.word_dict)
        self._get_rating_current(self, self.df, action_company)
        self._make_col_dict(self, self.word_dict, self.col_dict)
        if preprocess:  # change columns
            self._add_rc_columns(self, self.df, self.col_dict)

    @classmethod
    def _make_word_dict(cls, self, word_dict):
        """Make word dict and assign to class variable."""
        word_dict['pos'] = (['Buy', 'Outperform', 'Strong',
                             'Positive', 'Accumulate', 'Overweight'])
        word_dict['neut'] = (['Neutral', 'Hold', 'Equal', 'Perform',
                              'In-Line', 'Weight', 'Mixed'])
        word_dict['neg'] = ['Sell', 'Under', 'Reduce', 'Below', 'Negative']

    @classmethod
    def _get_rating_current(cls, self, df, action_company):
        """Get/clean rating_current frame analyst recs df."""
        rc_col = 'rating_current'
        r_counts, r_norm = False, False

        if action_company:
            r_counts = (df.mask('action_company', action_company)
                          .vc(subset=[rc_col]))
            r_norm = (df.mask('action_company', action_company)
                        .vc(subset=[rc_col], normalize=True))
        else:
            r_counts = df.vc(subset=[rc_col])
            r_norm = df.vc(subset=[rc_col], normalize=True)

        rc = r_counts.reset_index()[rc_col].tolist()

        self.r_norm = (r_norm.reset_index().set_index(rc_col)
                             .rename(columns={0: 'percs'})
                             .copy())

        self.r_counts = (r_counts.reset_index().set_index(rc_col)
                                 .rename(columns={0: 'percs'})
                                 .copy())
        self.rc = rc

    @classmethod
    def _make_col_dict(cls, self, word_dict, col_dict):
        """Make column dict for identifying words in word_dict."""
        col_dict['pos'] = self._list_comp_vc(self, 'pos', self.rc)
        col_dict['neg'] = self._list_comp_vc(self, 'neg', self.rc)
        col_dict['neut'] = self._list_comp_vc(self, 'neut', self.rc)

    @classmethod
    def _list_comp_vc(cls, self, key, rc):
        """List comprehension for value counts."""
        # Possible keys = ['pos', 'neg', 'neutral']
        cols = [c for w in self.word_dict[key] for c in rc if w in c or c in w]
        return cols

    @classmethod
    def _add_rc_columns(cls, self, df, col_dict):
        """Add rating current 1/hot cat columns."""
        for key, val in col_dict.items():
            df[f"rc_{key}"] = np.where(
                df['rating_current'].isin(val), 1, 0
            )

        self.df = df.copy()


 # %% codecell
