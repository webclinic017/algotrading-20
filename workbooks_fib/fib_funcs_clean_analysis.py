"""Functions for cleaning/analyzing fib data."""
# %% codecell
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np
from scipy.signal import find_peaks, peak_prominences

try:
    from scripts.dev.studies.add_study_cols import std_ann_deviation
    from scripts.dev.workbooks_fib.fib_funcs import read_clean_combined_all
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet, round_cols
except ModuleNotFoundError:
    from studies.add_study_cols import std_ann_deviation
    from workbooks_fib.fib_funcs import read_clean_combined_all
    from multiuse.help_class import baseDir, write_to_parquet, round_cols


# %% codecell


def fib_all_clean_combine_write(dt=False, read=False, round=True):
    """Take pre_cleaned_data and fib_vals. Combine for further analysis."""
    df_all = None
    bpath = Path(baseDir().path, 'ml_data/fib_analysis')
    fib_all_path = bpath.joinpath('fib_all_cleaned_data.parquet')
    fib_vals_path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals.parquet')

    if read:
        df_all = pd.read_parquet(fib_all_path)
    else:
        if not dt:
            dt = date(2021, 1, 1)
        df_pre = read_clean_combined_all(dt=dt)

        fib_df = pd.read_parquet(fib_vals_path)
        cols_to_rename = {'range': 'fib_range', 'date': 'fib_date'}
        fib_df.rename(columns=cols_to_rename, inplace=True)

        fib_cols = fib_df.columns
        fib_cols = (fib_cols[~fib_cols.isin(['symbol', 'date'])]
                    .append(pd.Index(['hit_1.618', 'hit_2.618', 'hit_4.236'])))

        df_drop = df_pre.drop(columns=fib_cols, errors='ignore').copy()
        df_all = pd.merge(df_drop, fib_df, on=['symbol'], how='left')

        write_to_parquet(df_all, fib_all_path)

    if round:
        cols_to_round = df_all.select_dtypes(include=[np.float32]).columns.tolist()
        df_all[cols_to_round] = df_all[cols_to_round].astype(np.float64)
        df_all[cols_to_round] = df_all[cols_to_round].round(3)

    return df_all

# %% codecell


def add_fib_peaks_troughs_diffs(read=False):
    """Apply distance matrix to each row. Find min differences. Local peaks/troughs."""
    df_all = None
    bpath = Path(baseDir().path, 'ml_data/fib_analysis')
    fib_all_path = bpath.joinpath('fib_all_cleaned_data.parquet')
    path_peaks_troughs = bpath.joinpath('fib_diff_peaks_troughs.parquet')

    if read:
        df_all = pd.read_parquet(path_peaks_troughs)
    else:
        df_clean = pd.read_parquet(fib_all_path)
        df_clean['fHigh_peaks'] = (np.where(df_clean.index.isin(
                                   find_peaks(df_clean['fHigh'])[0]), 1, 0))
        df_clean['fLow_troughs'] = (np.where(df_clean.index.isin(
                                   find_peaks(-df_clean['fLow'])[0]), 1, 0))

        cols_exclude = ['ext_date', 'ext_end', 'ext_cond']
        ext_ret_cols = ([col for col in df_clean.columns
                        if ((('ret_' in str(col)) | ('ext_' in str(col)))
                         & (str(col) not in cols_exclude))])

        dist_high_cols = ['symbol', 'date', 'fHigh'] + ext_ret_cols
        dist_low_cols = ['symbol', 'date', 'fLow'] + ext_ret_cols

        df_clean_high_dist = (df_clean[dist_high_cols].set_index(
                             ['symbol', 'date', 'fHigh']).copy())
        df_fHigh_dist = (abs(df_clean_high_dist.sub(df_clean_high_dist.index
                         .get_level_values('fHigh'), axis=0)))

        df_clean_low_dist = (df_clean[dist_low_cols].set_index(
                             ['symbol', 'date', 'fLow']).copy())
        df_fLow_dist = (abs(df_clean_low_dist.sub(df_clean_low_dist.index
                            .get_level_values('fLow'), axis=0)))

        df_clean_idx = df_clean.set_index(['symbol', 'date'])
        df_clean_idx['fibHighMinCol'] = (df_fHigh_dist.idxmin(axis='columns')
                                         .reset_index(level='fHigh', drop=True)
                                         )
        df_clean_idx['fibHighMinVal'] = (df_fHigh_dist.min(axis=1)
                                         .reset_index(level='fHigh', drop=True)
                                         )
        df_clean_idx['fibLowMinCol'] = (df_fLow_dist.idxmin(axis='columns')
                                        .reset_index(level='fLow', drop=True))
        df_clean_idx['fibLowMinVal'] = (df_fLow_dist.min(axis=1)
                                        .reset_index(level='fLow', drop=True))

        df_all = df_clean_idx.reset_index()

        df_all['fibHighDiffP'] = df_all['fibHighMinVal'].div(df_all['fHigh'])
        df_all['fibLowDiffP'] = df_all['fibLowMinVal'].div(df_all['fLow'])

        write_to_parquet(df_all, path_peaks_troughs)

    return df_all


# %% codecell


def fib_pp_cleaned(read=True, drop=True):
    """Calculate peak prominence, drop excess columns."""
    df_all = None
    bpath = Path(baseDir().path, 'ml_data/fib_analysis')
    path_peaks_troughs = bpath.joinpath('fib_diff_peaks_troughs.parquet')
    path_pp_cleaned = bpath.joinpath('fib_pp_cleaned.parquet')

    if read and path_pp_cleaned.exists():
        df_all = pd.read_parquet(path_pp_cleaned)
    else:
        # Read previous function dataframe
        df_all = pd.read_parquet(path_peaks_troughs)
        # Calculate peak prominences scores
        pph_idx = df_all[df_all['fHigh_peaks'] == 1].index
        pph = peak_prominences(df_all['fHigh'], pph_idx)
        ppl_idx = df_all[df_all['fLow_troughs'] == 1].index
        ppl = peak_prominences(-df_all['fLow'], ppl_idx)

        # Assign peak prominence scores to dataframe
        df_all['pp_fHigh'] = 0
        df_all.loc[pph_idx, 'pp_fHigh'] = pph[0]
        df_all['pp_fLow'] = 0
        df_all.loc[ppl_idx, 'pp_fLow'] = ppl[0]

        # Calculate cumulative percentage, percentage diff 5 rows forward
        df_all['prev_symbol'] = (df_all['symbol'].shift(periods=1, axis=0)
                                 .astype('category'))
        # Volume over avg of 2 months
        df_all['vol/avg2m'] = df_all['fVolume'].div(df_all['vol_avg_2m'])

        df_all = std_ann_deviation(df_all).copy()

        # df_all['cumPerc'] = np.where(
        #    df_all['symbol'] == df_all['prev_symbol'],
        #    df_all['fChangeP'].cumsum(),
        #    np.NaN)

        df_all['cumPerc'] = df_all.groupby(by=['symbol'])['fChangeP'].cumsum()

        df_all['perc1'] = np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-1))),
            abs(df_all['cumPerc'].shift(-1)) - abs(df_all['cumPerc']),
            np.NaN)

        df_all['perc2'] = np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-2))),
            abs(df_all['cumPerc'].shift(-2)) - abs(df_all['cumPerc']),
            np.NaN)

        df_all['perc3'] = np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-3))),
            abs(df_all['cumPerc'].shift(-3)) - abs(df_all['cumPerc']),
            np.NaN)

        df_all['perc5'] = np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-5))),
            df_all['cumPerc'].shift(-5) - df_all['cumPerc'],
            np.NaN)

        df_all['perc10'] = np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-10))),
            abs(df_all['cumPerc'].shift(-10)) - abs(df_all['cumPerc']),
            np.NaN)

        df_all['perc20'] = np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-20))),
            abs(df_all['cumPerc'].shift(-20)) - abs(df_all['cumPerc']),
            np.NaN)

        # Percentage difference between high (x days forward) and low
        df_all['h/l_perc1'] = (np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-1))),
            (df_all['fHigh'].shift(-1) - df_all['fLow']) / df_all['fLow'],
            np.NaN))

        df_all['h/l_perc2'] = (np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-2))),
            (df_all['fHigh'].shift(-2) - df_all['fLow']) / df_all['fLow'],
            np.NaN))

        df_all['h/l_perc3'] = (np.where(
            ((df_all['symbol'] == df_all['prev_symbol'])
             & (df_all['symbol'] == df_all['symbol'].shift(-3))),
            (df_all['fHigh'].shift(-3) - df_all['fLow']) / df_all['fLow'],
            np.NaN))

        # Assign one peak/trough for each row, rather than have both separate
        df_all['fibCloseVal'] = np.where(
            df_all['fibHighDiffP'] < df_all['fibLowDiffP'],
            df_all['fibHighDiffP'], df_all['fibLowDiffP'])

        df_all['fibCloseCol'] = np.where(
            df_all['fibHighDiffP'] < df_all['fibLowDiffP'],
            df_all['fibHighMinCol'], df_all['fibLowMinCol'])

        df_all['fibPP'] = np.where(
            df_all['fibHighDiffP'] < df_all['fibLowDiffP'],
            df_all['pp_fHigh'], df_all['pp_fLow'])

        df_all['fibCode'] = np.where(
            df_all['fibHighDiffP'] < df_all['fibLowDiffP'],
            'high', 'low')

        df_all['ifMinMax'] = np.where(
            df_all['fHigh_peaks'] == 1,
            'peak', np.NaN)
        df_all['ifMinMax'] = np.where(
            df_all['fLow_troughs'] == 1,
            'trough', df_all['ifMinMax'])

        # Define excess columns that may/not be dropped
        cols_to_drop = (['ret_0.001', 'ext_0.001',
                         'ret_0.236', 'ext_0.236', 'ret_0.382', 'ext_0.382',
                         'ret_0.5', 'ext_0.5', 'ret_0.618', 'ext_0.618',
                         'ret_0.786', 'ext_0.786', 'ret_0.999', 'ext_0.999',
                         'ret_1.236', 'ext_1.236', 'ret_1.5', 'ext_1.5',
                         'ret_1.618', 'ext_1.618', 'ret_2.618', 'ext_2.618',
                         'ret_4.236', 'ext_4.236',
                         'fHigh_peaks', 'fLow_troughs',
                         # 'fibHighMinCol', 'fibHighMinVal',
                         # 'fibLowMinCol', 'fibLowMinVal',
                         # 'fibHighDiffP', 'fibLowDiffP',
                         'pp_fHigh', 'pp_fLow'])

        if drop:  # Drop columns if specified
            df_all = df_all.drop(columns=cols_to_drop).copy()

        # Write to parquet
        write_to_parquet(df_all, path_pp_cleaned)

    # Return dataframe
    return df_all

# %% codecell


def fib_clean_to_view(df):
    """Drop low std devs, columns, and round remaining."""
    unique_stds = (df.sort_values(by=['stdDev'], ascending=True)
                     .drop_duplicates(subset=['symbol']))

    std_cutoff = unique_stds['stdDev'].describe()['25%']
    low_std_syms = (unique_stds[unique_stds['stdDev'] <= std_cutoff]
                               ['symbol'].tolist())
    df = df[~df['symbol'].isin(low_std_syms)].copy()

    cols_to_drop = (['fVolume', 'perc_2weeks', 'perc_1month', 'vol_avg_2m',
                     'fib_date', 'fib_range'])
    df_dropped = df.drop(columns=cols_to_drop, errors='ignore')

    # Exclude symbols with unusual (impossible) perc changes
    gap_down_error = (df_dropped[df_dropped['fChangeP'] < - .90]['symbol']
                      .unique().tolist())
    gap_up_error = (df_dropped[df_dropped['fChangeP'] > 15]['symbol']
                    .unique().tolist())
    errors_exclude = gap_down_error + gap_up_error
    df_dropped = df_dropped[~df_dropped['symbol'].isin(errors_exclude)]

    df_rounded = round_cols(df_dropped).copy()

    return df_rounded
