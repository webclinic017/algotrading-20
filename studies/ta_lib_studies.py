"""TA Lib Studies."""
# %% codecell
import pandas as pd
import pandas_ta as ta
import numpy as np

try:
    from scripts.dev.multiuse.symbol_ref_funcs import remove_funds_spacs
except ModuleNotFoundError:
    from multiuse.symbol_ref_funcs import remove_funds_spacs

# %% codecell


def get_ta_frame(df):
    """Convert dataframe into ta lib suitable df."""
    col_dict = ({'fOpen': 'open', 'fClose': 'close',
                 'fHigh': 'high', 'fLow': 'low',
                 'fVolume': 'volume'})

    col_list = ['symbol', 'date'] + [key for key in col_dict.keys()]
    df_idx = (df[col_list].rename(columns=col_dict)
              .set_index(['symbol', 'date'])
              .copy())

    return df_idx


# %% codecell


def add_ta_lib_studies(df, filter_cs=True, combine=True):
    """Add ta lib studies to df, return df."""
    df_idx = get_ta_frame(df)

    df_idx_cdl = df_idx.ta.cdl_pattern(name="all")
    df_idx_study = df_idx.ta.ttm_trend()
    # Join dataframes together
    df_comb = df_idx_cdl.join(df_idx_study)

    if filter_cs:
        all_cs_syms = remove_funds_spacs()
        df_comb = (df_comb[df_comb.index.get_level_values('symbol')
                   .isin(all_cs_syms['symbol'])].copy())

    if combine:
        df.set_index(['symbol', 'date'], inplace=True)
        df_comb = df.join(df_comb).reset_index()

    return df_comb


# %% codecell


def make_emas(df, combine=True):
    """Add ema columns with crossovers."""
    df_ta = get_ta_frame(df)

    ema_list = [10, 20, 50, 200]
    ema_dict = {f"ema{str(n)}": n for n in ema_list}
    gp = df_ta.groupby(level=['symbol'], as_index=False)

    for key, val in ema_dict.items():
        try:
            ema_dict[key] = gp.apply(lambda gp: gp.ta.ema(length=val))[0]
            ema_dict[key].name = key
            ema_dict[key] = ema_dict[key].reset_index(level=0, drop=True)
        except ValueError:
            pass
        except KeyError:
            pass

    ema_s_list = []
    for val in ema_dict.values():
        if isinstance(val, pd.Series):
            ema_s_list.append(val)

    df_emas = pd.concat(ema_s_list, axis=1)

    col_crossovers = []
    cross_dict = {}

    for col1, n1 in zip(df_emas.columns.tolist(), ema_list):
        for col2, n2 in zip(df_emas.columns.tolist(), ema_list):
            if (col1 != col2) and n1 < n2:
                col_crossovers.append((col1, col2))
                cross_dict[col1] = col2

    for tup in col_crossovers:
        col = f"{tup[0]}_{tup[1]}"
        df_emas[col] = df_emas[tup[0]].sub(df_emas[tup[1]])
        df_emas[f"{tup[0]}_x_{tup[1]}"] = np.where(
            (((np.sign(df_emas[col]) < 0) &
             (np.sign(df_emas[col].shift(1, axis=0)) > 0))), -1,
            np.where(
             ((np.sign(df_emas[col]) > 0) &
              (np.sign(df_emas[col].shift(1, axis=0)) < 0)), 1, 0)
        )

    if combine:
        df_emas = (pd.merge(df, df_emas, left_on=['symbol', 'date'],
                            right_index=True, how='left'))
    return df_emas


# %% codecell
