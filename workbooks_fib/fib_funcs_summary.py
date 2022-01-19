"""Summary functions for fibonacci sequence analysis."""
# %% codecell
import pandas as pd
import numpy as np

from tqdm import tqdm

# %% codecell


def fib_peak_trough_summary(df, cols_to_add=[]):
    """Get summary view of peak, troughs."""
    cols_to_view = (['symbol', 'annStdDev', 'date_range', 'start_date',
                     'ext_date', 'ext_end'])
    if cols_to_add:
        cols_to_view = cols_to_view + cols_to_add

    # Get count of all peaks/troughs
    imm_count = (df[df['ifMinMax'] != 'nan'][['symbol', 'ifMinMax']]
                 .groupby(by=['symbol']).count())
    # Get sum of all percDiffs for peak/trough rows
    pdiff_sum = (df[df['ifMinMax'] != 'nan'][['symbol', 'fibCloseVal']]
                 .groupby(by=['symbol']).sum())

    gp_eval = (pd.merge(imm_count, pdiff_sum,
                        right_index=True, left_index=True))
    # Remove symbols where no peaks/troughs exist, where percDiff = 0
    gp_eval = (gp_eval[(gp_eval['ifMinMax'] > 0)
               & (gp_eval['fibCloseVal'] > 0)].copy())
    # Get average closeness based on sum of percDiff/count of peaks/troughs
    gp_eval['avgClose'] = (gp_eval['fibCloseVal']
                           .div(gp_eval['ifMinMax']))

    std_idx = (df[cols_to_view].drop_duplicates(subset=['symbol'])
                               .set_index('symbol'))
    gp_eval = (pd.merge(gp_eval, std_idx, left_index=True,
                        right_index=True, how='left'))

    # gp_eval.sort_values(by=['annStdDev'], ascending=False).head(50)

    return gp_eval

# %% codecell


def trough_over_peak_summary(df, col_to_calc='fibCloseCol'):
    """Calculate trough/peak summary stats."""
    cond1 = (df['date'] > df['end_date'])
    cond2 = (df['fibHighDiffP'] < .05)
    cond3 = (df['fibLowDiffP'] < .05)
    df = df[cond1 & cond2 & cond3].copy()

    fib_min_max = (df[df['ifMinMax'] != 'nan']
                   [[col_to_calc, 'ifMinMax']]
                   .value_counts(sort=False).reset_index()
                   .rename(columns={0: 'counts'}))

    fib_min_max['t/p'] = np.where(
        fib_min_max['ifMinMax'] == 'trough',
        fib_min_max['counts'].div(fib_min_max['counts'].shift(1)),
        np.NaN
    )

    return fib_min_max

# %% codecell


def bounce_perf_cutoffs(df, cols_to_look=[]):
    """Filter data. Come up with summary perf description for fib levels."""

    if not cols_to_look:
        # cols_to_look = (['ret_0.999', 'ret_1.236', 'ret_1.5',
        #                 'ret_1.618', 'ret_2.618', 'ret_4.236', 'ext_0.001'])
        cols_to_look = df['fibCloseCol'].value_counts().index.tolist()

    df_bounce = df[df['fibCloseCol'].isin(cols_to_look)].copy()

    # Only look at dates after fib_seq end date
    df_after = df_bounce[df_bounce['date'] > df_bounce['ext_date']].copy()
    df_after = df_after[df_after['fibCloseVal'] < .05]
    df_after = df_after.drop_duplicates(subset=['symbol', 'fibHighMinCol'])
    df_after = df_after.drop_duplicates(subset=['symbol', 'fibLowMinCol'])

    # Still a problem with symbols that aren't actively traded
    syms_to_exclude = (['AADI', 'ZYXI'])
    df_excluded = df_after[~df_after['symbol'].isin(syms_to_exclude)].copy()

    return df_excluded


def run_fib_perf_for_loop(df, cols_to_look=[]):
    """Run fibonacci level performance for loop. Return summary."""
    if not cols_to_look:
        cols_to_look = df['fibCloseCol'].value_counts().index.tolist()

    sum_list = []
    perc_cols = [col for col in df.columns if 'perc' in col]
    high_cols = ['fibHighMinCol'] + perc_cols
    low_cols = ['fibLowMinCol'] + perc_cols

    min_val_cuts = [.01, .02, .03, .04, .05]

    for col in tqdm(cols_to_look):
        for cut in min_val_cuts:
            ret_test = (df[((df['fibHighMinCol'] == col) |
                            (df['fibLowMinCol'] == col)) &
                           ((df['fibHighMinVal'] < cut) |
                            (df['fibLowMinVal'] < cut))].copy()
                        [['fibHighMinCol', 'fibLowMinCol'] + perc_cols])

            ret_high = (ret_test[ret_test['fibHighMinCol'] == col]
                        [high_cols].describe().T
                        .reset_index().rename(columns={'index': 'percs'})
                        .copy())
            ret_high.insert(0, 'high/low', 'high')
            ret_high.insert(0, 'fib_level', col)
            ret_high.insert(0, 'cutoff', cut)

            sum_list.append(ret_high)

            ret_low = (ret_test[ret_test['fibLowMinCol'] == col]
                       [low_cols].describe().T
                       .reset_index().rename(columns={'index': 'percs'})
                       .copy())
            ret_low.insert(0, 'high/low', 'low')
            ret_low.insert(0, 'fib_level', col)
            ret_low.insert(0, 'cutoff', cut)

            sum_list.append(ret_low)

    df_bsum = pd.concat(sum_list)
    # Bounce summary
    bsum_groups = (df_bsum.groupby(by=['cutoff', 'percs',
                                       'fib_level', 'high/low'])
                          .mean())
    return bsum_groups
