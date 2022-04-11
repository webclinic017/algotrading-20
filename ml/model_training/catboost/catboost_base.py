"""Catboost base."""
# %% codecell
from pathlib import Path

import pandas as pd
import numpy as np

from sklearn.model_selection import TimeSeriesSplit
from catboost import Pool, CatBoostRegressor, CatBoostClassifier
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg

# %% codecell


def make_ml_training_dict():
    """Make the ml training dict for different datasets."""
    data_dict = ({
        'yanalysis': {
            'fdir': Path(baseDir().path, 'ml_data', 'ml_training'),
            'fpath': 'yanalysis.parquet',
            'ycol': 'changePercent',
            'cols_to_drop': ['changeOverTime', 'changePercent'],
            'cat_features': ['symbol']
        }
    })

    return data_dict

# %% codecell


class CatBoostBase():
    """Catboost data prep and model fit."""

    data_dict = make_ml_training_dict()

    # data_method : keyword arg : to merge ml training dict with kwargs

    def __init__(self, **kwargs):
        self.df_data = self._cbb_class_vars(self, **kwargs)
        self._cbb_time_series_split(self, self.df_data)
        if not self.stop_at_split:
            self.cb_params = self._cbb_model_params(self)
            self.model_result = self._cbb_model_define_fit(self)

    @classmethod
    def _cbb_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')
        self.print_model_progress = kwargs.get('print_model_progress')
        self.stop_at_split = kwargs.get('stop_at_split')
        self.one_hot = kwargs.get('one_hot')
        # data_method fits into self.data_dict
        data_method = kwargs.get('data_method')

        if data_method in self.data_dict.keys():
            kwargs = kwargs | self.data_dict[data_method]
            # Joinpath to get fpath
            kwargs['fpath'] = kwargs['fdir'].joinpath(kwargs['fpath'])

        # Should only be one column name
        self.ycol = kwargs.get('ycol')
        # Drop any columns from dataframe
        self.cols_to_exclude = kwargs.get('cols_to_exclude', [])
        # Will also assume any object/string column is cat, along with
        # any column name that includes date
        self.cat_features = kwargs.get('cat_features')

        df_data = kwargs.get('df', kwargs.get('fpath'))
        # If dataframe not in kwargs, look for fpath
        if not isinstance(df_data, pd.DataFrame):
            # Here, df data is just an fpath
            if Path(df_data).exists():
                df_data = pd.read_parquet(df_data).copy()

        # catboost params
        self.catboost_params = kwargs.get('catboost_params')
        # training_dir_output
        self.fdir_train = (Path(baseDir().path, 'ml_data',
                           'training_model_output', 'catboost'))
        if not self.fdir_train.exists():
            self.fdir_train.mkdir(mode=0o777, parents=True)

        # Assume self.df_data exists by this point
        return df_data

    @classmethod
    def _cbb_time_series_split(cls, self, df_data, **kwargs):
        """Time series split from sklearn."""
        if self.verbose:
            help_print_arg(f"""cols_to_drop: {str(self.cols_to_exclude)}
                               ycol: {str(self.ycol)}""")

        if self.stop_at_split and self.one_hot:  # One hot encode
            ocols = df_data.select_dtypes(['object']).columns
            df_data = (df_data.join(pd.get_dummies(
                       df_data.select_dtypes(['object'])))
                       .drop(columns=ocols)
                       .copy())
        elif self.stop_at_split and not self.one_hot:
            cols_to_str = df_data.select_dtypes(['object', 'category']).columns.tolist()
            df_data[cols_to_str] = df_data[cols_to_str].astype('category')

            cat_dict = {key: '' for key in cols_to_str}
            for key in cat_dict.keys():
                try:
                    cats = pd.factorize(df_data[key].cat.categories)
                    cat_sub = {key: val for key, val in zip(cats[1], cats[0])}
                    cat_dict[key] = cat_sub
                    df_data[key] = df_data[key].map(cat_dict[key])
                except AttributeError:
                    print(f"_categorical_encoding: {key} not a categorical variable")

        # Time series split to avoid lookahead bias
        test_size_len = int(len(df_data) * .20)
        tscv = TimeSeriesSplit(n_splits=3, test_size=test_size_len)

        df_x = df_data.drop(columns=self.cols_to_exclude, errors='ignore')
        df_y = df_data[self.ycol]
        # Drop self.ycol if it's in the training database
        if self.ycol in df_x.columns:
            df_x.drop(columns=self.ycol, errors='ignore', inplace=True)

        for train_index, test_index in tscv.split(df_x):
            break
        # Split data into train/test sets
        train_data, train_y = df_x.loc[train_index], df_y.loc[train_index]
        test_data, test_y = df_x.loc[test_index], df_y.loc[test_index]

        # Assign class variables
        self.train_data = train_data
        self.train_y = train_y
        self.test_data = test_data
        self.test_y = test_y
        if not self.stop_at_split:  # One hot encode
            self.train_pool = (Pool(train_data, train_y, cat_features=self.cat_features))
            self.test_pool = (Pool(test_data, test_y, cat_features=self.cat_features))

    @classmethod
    def _cbb_model_params(cls, self):
        """Unpack catboost params if they exist, if not, define defaults."""
        catboost_model_params = ({
            'Regressor': {
                'iterations': 500,
                'depth': 10,
                'learning_rate': 1,
                'loss_function': 'RMSE'
            },
            'Classifier': {
                'iterations': 2,
                'depth': 2,
                'learning_rate': 1,
                'loss_function': 'LogLoss'
            }
        })
        # If catboost parameters were not passed in through the args
        if not self.catboost_params:
            self.catboost_params = catboost_model_params
        # Decide between catboost regressor/categorical
        cat_dtypes = ('object', 'string', 'category')
        if self.test_y.dtype not in cat_dtypes:
            self.catboost_method = 'Regressor'
            self.catboost_params = self.catboost_params['Regressor']
            self.model = CatBoostRegressor
            print('CatBoostBase: Using CatBoostRegressor')
        else:
            self.catboost_method = 'Classifier'
            self.catboost_params = self.catboost_params['Classifier']
            self.model = CatBoostClassifier
            print('CatBoostBase: Using CatBoostClassifier')

        if self.verbose:
            print(f"Catboost model params {str(self.catboost_params)}")

    @classmethod
    def _cbb_model_define_fit(cls, self, **kwargs):
        """Define model and fit to training pool."""
        model = self.model(**self.catboost_params)
        model.fit(self.train_pool, verbose=self.print_model_progress)
        self.model = model

        preds = model.predict(self.test_pool)
        metric_strs = []
        if self.catboost_method == 'Regressor':
            rmse = (np.sqrt(mean_squared_error(self.test_y, preds)))
            r2 = r2_score(self.test_y, preds)

            metric_strs.append(rmse)
            metric_strs.append(r2)
            print('RMSE: {:.2f}'.format(rmse))
            print('R2: {:.2f}'.format(r2))

        model_result = ({
            'model': model,
            'preds': preds,
            'metrics': metric_strs
        })

        return model_result
