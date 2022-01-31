"""User defined panda functions."""
# %% codecell

import pandas as pd

# %% codecell


def mask(df, key, value, equals=True, not_equals=False, greater=False, lesser=False, notin=False):
    """Modified mask."""
    # If comparing rows values to singular value
    if lesser or greater:
        equals = False

    if not isinstance(value, list):
        if equals:
            df = df[df[key] == value]
        elif not_equals:
            df = df[df[key] != value]
        elif greater:
            df = df[df[key] > value]
        elif lesser:
            df = df[df[key] < value]
    # If user passes a list of values for in/notin
    elif len(value) > 1:
        if notin:
            df = df[~df[key].isin(value)]
        else:
            df = df[df[key].isin(value)]

    return df


def chained_isin(df, key, notin=False, nlargest=False, nsmallest=False, idx_diff=False):
    """Modified isin function."""
    if nlargest:
        if notin:
            df = df[~df[key].isin(df[key].nlargest(nlargest))].copy()
        else:
            df = df[df[key].isin(df[key].nlargest(nlargest))].copy()
    elif nsmallest:
        if notin:
            df = df[~df[key].isin(df[key].nsmallest(nsmallest))].copy()
        else:
            df = df[df[key].isin(df[key].nsmallest(nsmallest))].copy()

    if idx_diff:
        df.insert(2, 'idx_diff', df.index)
        df['idx_diff'] = df['idx_diff'].diff(periods=-1)
        df['idx_diff'].fillna(1, inplace=True)

    return df

pd.DataFrame.mask = mask
pd.DataFrame.chained_isin = chained_isin

# %% codecell
