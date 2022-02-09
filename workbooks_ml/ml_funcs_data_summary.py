"""Data summary functions."""
# %% codecell
from pathlib import Path
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

# %% codecell


def create_summary_df_ml(df):
    """Create a summary dataframe to use for ml studies."""
    df_types = pd.DataFrame(df.dtypes, columns=['column_dtype'])

    result = []
    for col in df_types.index:
        try:
            result.append((col, (df[col] < 0).values.any()))
        except TypeError:
            pass

    df_neg = (pd.DataFrame.from_records(result)
              .rename(columns={0: 'columns', 1: 'has_negatives'})
              .set_index('columns'))

    df_sum = (pd.concat([df_types, df.count(),
                         df_neg.squeeze(axis=1)], axis=1))
    df_sum.rename(columns={0: 'non-na_count'}, inplace=True)
    df_skews = df.skew(axis=0, skipna=True, numeric_only=True)
    df_skews.name = 'skew'

    df_comb = (pd.merge(df_sum, df_skews, left_index=True,
                        right_index=True, how='left'))

    bpath = Path(baseDir().path, 'ml_data', 'ml_training')
    exc_path = bpath.joinpath('_fib_cleaned_sum.xlsx')
    csv_path = bpath.joinpath('_fib_cleaned_sum.csv')

    df_comb.to_excel(exc_path)
    df_comb.to_csv(csv_path)

    return df_comb

# %% codecell
