"""Tranform datetime and categorical columns."""
# %% codecell

from pathlib import Path

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_parquet

# %% codecell


class DTCategoricalTransform():
    """Preprocess datetime and categorical data."""

    cat_dict = {}
    df = pd.DataFrame()

    def __init__(self, df):
        self._drop_cols(self, df)
        self._categorical_encoding(self, self.df)
        self._convert_date_cols_numeric(self, self.df)
        self._write_categorical_dict_to_df(self, self.cat_dict)

    @classmethod
    def _drop_cols(cls, self, df):
        """Drop useless columns."""
        cols_to_drop = (['adj close', 'prev_symbol',
                         'start', 'high', 'ext_end'])
        self.df = df.drop(columns=cols_to_drop, errors='ignore')

    @classmethod
    def _categorical_encoding(cls, self, df):
        """Categorically encode columns and set values."""
        cat_cols = (['cond', 'ext_cond', 'fibCloseCol', 'fibCode',
                     'fibHighMinCol', 'fibLowMinCol', 'ifMinMax', 'symbol'])

        cat_dict = {key: '' for key in cat_cols}

        for key in cat_dict.keys():
            cats = pd.factorize(df[key].cat.categories)
            cat_sub = {key: val for key, val in zip(cats[1], cats[0])}
            cat_dict[key] = cat_sub
            df[key] = df[key].map(cat_dict[key])

        self.df = df
        self.cat_dict = cat_dict

    @classmethod
    def _convert_date_cols_numeric(cls, self, df, dropcols=True, combine=True):
        """Convert datetime columns to numeric."""
        date_cols = (['date', 'start_date', 'end_date',
                      'fib_date', 'ext_date'])
        df_dates = df[date_cols].copy()

        for dt in date_cols:
            df_dates[f"{dt}_dayofyear"] = df_dates[dt].dt.dayofyear
            df_dates[f"{dt}_month"] = df_dates[dt].dt.month
            df_dates[f"{dt}_dayofweek"] = df_dates[dt].dt.isocalendar().week
            df_dates[f"{dt}_year"] = df_dates[dt].dt.year

        if combine:
            df_dates = df.drop(columns=date_cols).join(df_dates)
        if dropcols:
            df_dates.drop(columns=date_cols, inplace=True)

        self.df = df_dates.copy()

    @classmethod
    def _write_categorical_dict_to_df(cls, self, cat_dict):
        """Convert dictionary to dataframe and write locally."""
        key_list = cat_dict.keys()
        val_list = []
        for key in key_list:
            for key2, val in cat_dict[key].items():
                val_list.append((key, key2, val))

            # print(dtc.cat_dict[key])
            # break

        col_list = ['column', 'category', 'code']
        df_catkeys = pd.DataFrame(val_list, columns=col_list)
        bpath = Path(baseDir().path, 'ml_data', 'ml_training')
        fpath = bpath.joinpath('_df_catkeys.parquet')

        self.cat_dict_fpath = fpath

        write_to_parquet(df_catkeys, fpath)


# %% codecell
