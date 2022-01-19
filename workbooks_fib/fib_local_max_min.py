"""Fib workbook for identifying local max/min values."""
# %% codecell
from pathlib import Path
import importlib
import sys

from tqdm import tqdm
import pandas as pd
import numpy as np

from scipy.signal import find_peaks, peak_prominences

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet, round_cols
    from scripts.dev.workbooks_fib.fib_funcs_clean_analysis import fib_all_clean_combine_write
    from scripts.dev.studies.add_study_cols import std_ann_deviation
    from scripts.dev.multiuse.symbol_ref_funcs import get_symbol_stats
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet, round_cols
    from workbooks_fib.fib_funcs_clean_analysis import fib_all_clean_combine_write
    from studies.add_study_cols import std_ann_deviation
    from multiuse.symbol_ref_funcs import get_symbol_stats
    from api import serverAPI

# %% codecell

from workbooks_fib.fib_funcs_clean_analysis import (
    fib_all_clean_combine_write, add_fib_peaks_troughs_diffs,
    fib_pp_cleaned, fib_clean_to_view)

importlib.reload(sys.modules['workbooks_fib.fib_funcs_clean_analysis'])

from workbooks_fib.fib_funcs_clean_analysis import (
    fib_all_clean_combine_write, add_fib_peaks_troughs_diffs,
    fib_pp_cleaned, fib_clean_to_view)


from workbooks_fib.fib_funcs_summary import (fib_peak_trough_summary,
    bounce_perf_cutoffs, run_fib_perf_for_loop)

importlib.reload(sys.modules['workbooks_fib.fib_funcs_summary'])

from workbooks_fib.fib_funcs_summary import (fib_peak_trough_summary,
    bounce_perf_cutoffs, run_fib_perf_for_loop)
# %% codecell

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 150)
# %% codecell

df_all = fib_all_clean_combine_write(read=False)
df_peak_troughs = add_fib_peaks_troughs_diffs(read=True).copy()
df_cleaned = fib_pp_cleaned(read=False, drop=False).copy()

df_cleaned = fib_clean_to_view(df_cleaned).copy()
# %% codecell

df_cleaned

# syms_to_check = get_symbol_stats()['symbol'].unique()
# df_cleaned = df_cleaned[df_cleaned['symbol'].isin(syms_to_check)].copy()


# %% codecell
df_except = bounce_perf_cutoffs(df_cleaned).copy()

# ext_cond
ext_conds = ['fib618', 'range>start']
df_excluded = df_except[df_except['ext_cond'].isin(ext_conds)]


df_cond_bsum = run_fib_perf_for_loop(df_excluded)

df_bsum = run_fib_perf_for_loop(df_except)
df_max_cut = df_except[df_except['cumPerc'] < 3].copy()
df_bsum_max_cut = run_fib_perf_for_loop(df_max_cut)

#

df_bsum_max_cut[(df_bsum_max_cut['count'] > 5) & (df_bsum_max_cut['mean'].isin(df_bsum_max_cut['mean'].nlargest(15)))]

df_hexcept = bounce_perf_cutoffs(df_cleaned).copy()
hcond1 = (df_except['fibHighMinCol'] == 'ext_4.236')
hcond2 = (df_except['fibHighDiffP'] < .01)

# %% codecell
df_h = df_hexcept[hcond1 & hcond2].copy()
perc_cols = [col for col in df_h.columns if 'perc' in col]
desc_list = []

cuts = np.arange(-.25, 0, .01)
for cut in tqdm(cuts):
    for col in perc_cols:
        df_h.loc[:, col] = np.where(df_h[col] < cut, cut, df_h[col])

    desc = df_h[perc_cols].describe().T.reset_index().rename(columns={'index': 'percs'}).copy()
    desc['cut'] = cut
    desc_list.append(desc)

desc_all = pd.concat(desc_list)

# %% codecell
df_h = df_hexcept[hcond1 & hcond2].copy()
perc_cols = [col for col in df_h.columns if 'perc' in col]
for col in perc_cols:
    df_h.loc[:, col] = np.where(df_h[col] < cut, cut, df_h[col])

df_h[perc_cols].describe()


df_h


cond1 = (df_except['fibLowMinCol'] == 'ret_4.236')
cond2 = (df_except['fibLowDiffP'] < .04)
# cond5 = (df_max_cut['fibCloseVal'] < .05)

df_tperf = df_except[(cond1 & cond2)].copy()

df_bsum[(df_bsum['count'] > 5) & (df_bsum['mean'].isin(df_bsum['mean'].nlargest(20)))]

df_bsum[(df_bsum['count'] > 5) & (df_bsum.index.get_level_values('high/low') == 'low') & (df_bsum['mean'].isin(df_bsum['mean'].nlargest(30)))]

# 2 major things:
# Testing whether fibonacci sequences are still applicable
# (if price outside window)

# Checking for intraday fibonacci sequences that are applicable

# %% codecell
col_check = 'ext_2.618'
cutoff = .02
econd1 = (df_except['fibLowMinCol'] == col_check)
econd2 = (df_except['fibLowMinVal'] < cutoff)
econd3 = (df_except['fibHighMinCol'] == col_check)
econd4 = (df_except['fibHighMinVal'] < cutoff)

df_except_conds = df_except[(econd1 | econd3) & (econd2 | econd4)].copy()
df_except_conds = df_except_conds[df_except_conds['fibLowMinCol'] == col_check]

# df_except_conds = df_except_conds[df_except_conds['fibHighMinVal'] < cutoff]

# df_except_conds = df_except_conds[df_except_conds['fibLowMinCol'] != col_check]

perc_cols = [col for col in df_except.columns if 'perc' in col]

df_except_conds[df_except_conds['fibHighMinCol'] != col_check].shape
df_except_conds[df_except_conds['fibLowMinCol'] != col_check].shape

df_except_conds.shape

df_except_conds[perc_cols].describe().T

perc_cols_rev = perc_cols.copy()
perc_cols_rev.reverse()
cols_to_view = ['symbol', 'date', 'start_date', 'ext_date', 'ext_end', 'ext_cond', 'cond', 'fHighMax', 'cumPerc', 'rsi', 'fibPercRange']
cols_to_view = cols_to_view + perc_cols_rev

# df_except_conds.insert(1, 'ext_to_acc', getDate.get_bus_day_diff(df_except_conds, 'ext_date', 'date'))
df_except_conds['ext_to_acc'].describe()

df_except_conds.groupby(by=['cond'])[perc_cols_rev].mean()
df_except['ext_cond'].value_counts()

df_except.groupby(by=['ext_cond'])[perc_cols_rev].mean()

df_except_conds[cols_to_view]

# %% codecell




# %% codecell

from datetime import date
sym = 'ZIVO'
dt = '2021-05-28'

df_cleaned[(df_cleaned['symbol'] == sym) & (df_cleaned['date'] == dt)]

# %% codecell

# Still a problem with symbols that aren't actively traded

# ext_cond
# ext_conds = ['fib618', 'range>start']
# df_excluded = df_excluded[df_excluded['ext_cond'].isin(ext_conds)]
# %% codecell

from pandas.tseries.offsets import BusinessDay
cols_to_view = (['symbol', 'date', 'date_range', 'start_date', 'end_date',
                 'ext_date', 'ext_end',
                 'fClose', 'ext_cond', 'cond', 'cumPerc', 'rsi',
                 'fibPercRange', 'fibLowMinCol'])


# %% codecell
cols_to_look = (['ret_0.999', 'ret_1.236', 'ret_1.5',
                 'ret_1.618', 'ret_2.0', 'ret_2.618', 'ret_3.0',
                 'ret_4.236', 'ext_0.001'])
bcond1 = (df_cleaned['fibLowMinCol'].isin(cols_to_look))
bcond2 = (df_cleaned['fibLowDiffP'] < .05)

df_low_bounce = (df_cleaned[(bcond1 & bcond2)]
                 .drop_duplicates(subset=['symbol', 'fibLowMinCol'])
                 .copy())

for n in range(5):
    dt = getDate.query('iex_eod') - BusinessDay(n)
    df_lb = df_low_bounce[df_low_bounce['date'] == str(dt)].copy()
    if not df_lb.empty:
        break
# %% codecell

# Fix fiboancci formulation for ARBE - should've picked up
# the 2 days before

# It's possible that you need two touch points before stock goes up

# BPMC - drawdown
# CALA could be interesting. Touched

# IQVA - big red down, fLow should be included in fib

df_apre = df_cleaned[df_cleaned['symbol'] == 'APRE']


df_lb[cols_to_view]

df_cleaned.columns

# %% codecell

# %% codecell


# ext_cond


# The code to find a SQ type situation is fairly simple:
# Look for a ret level that hasn't yet been hit:
# 261.8 and 4.36. Then find the closest that it's come to hit that level
# Get an alert as soon as this happens.

# It would also be interesting to see what the candlestick type,
# volume/avg compared for local max/min types.

df_perc_diff_1 = df_cleaned[df_cleaned['fibCloseVal'] < .01]
df_perc_diff_1[['symbol', 'fibCloseCol', 'stdDev']].value_counts()

cols_to_drop = (['fVolume', 'perc_2weeks', 'perc_1month', 'vol_avg_2m',
                 'fib_date', 'fib_range'])
df_perc_diff_1 = df_perc_diff_1.drop(columns=cols_to_drop)

# How long does a stock stay around ret_1.236 before moving up or down?

df_perc_diff_1[df_perc_diff_1['ifMinMax'] != 'nan'][['fibCloseCol', 'ifMinMax']].value_counts(sort=False)


# %% codecell

from workbooks_fib.fib_funcs_summary import trough_over_peak_summary

# %% codecell
tps = trough_over_peak_summary(df_cleaned)

cond1 = (df_cleaned['date'] > df_cleaned['end_date'])
cond2 = (df_cleaned['fibHighDiffP'] < .05)
cond3 = (df_cleaned['fibLowDiffP'] < .05)
df_except = df_cleaned[cond1 & cond2 & cond3].copy()

df_except1 = df_except.drop_duplicates(subset=['symbol', 'fibCloseCol'])
df_except1_fLow = df_except.drop_duplicates(subset=['symbol', 'fibLowMinCol'])
df_except1_fHigh = df_except.drop_duplicates(subset=['symbol', 'fibHighMinCol'])

df = df_except1.copy()
fib_min = (df[df['ifMinMax'] != 'nan']
           [['fibLowMinCol', 'ext_cond', 'ifMinMax']]
               .value_counts(sort=False).reset_index()
               .rename(columns={0: 'counts'}))

fib_min_max = fib_min.copy()
fib_min_max['t/p'] = np.where(
    fib_min_max['ifMinMax'] == 'trough',
    fib_min_max['counts'].div(fib_min_max['counts'].shift(1)),
    np.NaN
)


df_cleaned


df_cleaned['ext_cond'].value_counts()
fib_min_max.tail(100)
tps
# Get only rows with less than 1% perc difference between val and nearest fib val

# Get all predictions that were out of range: ~1.5% diff to closest val
df_preds_out = df_cleaned[df_cleaned['fibCloseVal'] > .015]
df_preds_bef = df_preds_out[df_preds_out['date'] >= df_preds_out['start_date']].copy()

# %% codecell


# %% codecell


# %% codecell
