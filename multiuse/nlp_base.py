"""NLP helpers/base for common operations."""
# %% codecell

import numpy as np


# %% codecell

class NlpHelpers():
    """NLP helpers for repeat operations."""

    @staticmethod
    def get_all_uppercase_proportion(df, column):
        """Get proportion of words with all uppercase in 'text' column."""

        df_upper = (df[column].str.split(expand=True).stack().str.isupper()
                    .replace(False, np.NaN).dropna()
                    .reset_index(level=1, drop=True)
                    .reset_index()
                    .groupby(by='index').count()
                    .rename(columns={0: 'upperCount'})
                    .copy())

        df_wordcount = (df[column].str.split(expand=True)
                        .stack()
                        .reset_index(level=1, drop=True)
                        .reset_index()
                        .groupby(by='index').count()
                        .rename(columns={0: 'wordCount'}))

        df_counts = df.join(df_upper).join(df_wordcount).copy()
        df_counts['upper%'] = (df_counts['upperCount']
                               .div(df_counts['wordCount']))

        return df_counts
