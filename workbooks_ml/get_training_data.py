"""Recombine data."""
# %% codecell
from io import BytesIO

import pandas as pd
import requests


# %% codecell


class GetTrainingData():
    """Get training data from api."""
    baseUrl = 'https://algotrading.ventures/api/v1/data/ml'

    def __init__(self):
        self.rep_training = self._get_data(self, 'training')
        self.rep_ref = self._get_data(self, 'ref')

        self.df = self._convert_to_df(self, self.rep_training)
        self.df_ref = self._convert_to_df(self, self.rep_ref)
        self.df_dec = self._decode_data(self, self.df, self.df_ref)

        print('Categorical encoded dataframe : .df')
        print('Categorical keys : .df_ref')
        print('Categorically decoded data: .df_dec')

    @classmethod
    def _get_data(cls, self, method):
        """Get training data."""
        if method == "training":
            url = f"{self.baseUrl}/subset"
        elif method == "ref":
            url = f"{self.baseUrl}/subset_ref"

        get = requests.get(url)
        get.raise_for_status()
        return get

    @classmethod
    def _convert_to_df(cls, self, rep):
        """Convert to df, return dataframe."""
        df = pd.read_parquet(BytesIO(rep.content))
        return df

    @classmethod
    def _decode_data(cls, self, df_data, df_cat):
        """Decode categorically encoded data from keys."""
        decode_dict = (df_cat.groupby(by='column')[['category', 'code']]
                             .apply(lambda x: x.set_index('code').to_dict())
                             .to_dict())
        for key in decode_dict.keys():
            decode_dict[key] = decode_dict[key]['category']

        df_dec = df_data.replace(to_replace=decode_dict)

        return df_dec


# %% codecell
