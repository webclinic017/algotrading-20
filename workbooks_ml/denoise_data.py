"""Denoising/decomposing data."""
# %% codecell
from pathlib import Path
import importlib
import sys
import warnings
import typing
from datetime import date

import pandas as pd
from pykalman import KalmanFilter
import pywt
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet, df_create_bins
    from scripts.dev.multiuse.symbol_ref_funcs import remove_funds_spacs
    from scripts.dev.pre_process_data.scale_numericals import ScaleTransform
    from scripts.dev.pre_process_data.transform_dt_cat import DTCategoricalTransform
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet, df_create_bins
    from multiuse.symbol_ref_funcs import remove_funds_spacs
    from pre_process_data.scale_numericals import ScaleTransform
    from pre_process_data.transform_dt_cat import DTCategoricalTransform
    from api import serverAPI

importlib.reload(sys.modules['multiuse.help_class'])

# %% codecell
warnings.filterwarnings('ignore')
sns.set_style('whitegrid')
idx = pd.IndexSlice

pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 30)

# %% codecell

from multiuse.symbol_ref_funcs import remove_funds_spacs

syms_cs = remove_funds_spacs()


# I think the easiest thing to do here is to schedule
# 120 tasks for every minute

# %% codecell


# %% codecell

bpath = Path(baseDir().path, 'ml_data', 'ml_training')
path = bpath.joinpath('_fib_cleaned.parquet')
df_recent = pd.read_parquet(path)

dtc = DTCategoricalTransform(df_recent)
stf = ScaleTransform(dtc.df)

catkeys_fpath = bpath.joinpath('_df_catkeys.parquet')
df_catkeys = pd.read_parquet(catkeys_fpath)

df_processed = stf.df

process_fpath = bpath.joinpath('_df_processed.parquet')
write_to_parquet(df_processed, process_fpath)


# %% codecell


kf = KalmanFilter(transition_matrices = [1],
                  observation_matrices = [1],
                  initial_state_mean = 0,
                  initial_state_covariance = 1,
                  observation_covariance=1,
                  transition_covariance=.01)
# %% codecell

df_test = pd.read_parquet(fpath)
df_aapl = df_test[df_test['symbol'] == 'AAPL'].set_index('date')

state_means, _ = kf.filter(df_aapl['fClose'])

# %% codecell
# aapl_smoothed = aapl.to_frame('close')
aapl_smoothed = df_aapl['fClose'].to_frame()
aapl_smoothed['Kalman Filter'] = state_means

for months in [1,2,3]:
    aapl_smoothed[f'MA ({months}m)'] = df_aapl['fClose'].rolling(window=months*21).mean()

ax = aapl_smoothed.plot(title='Kalman Filter vs Moving Average', figsize=(14,6), lw=1, rot=0)
ax.set_xlabel('')
ax.set_ylabel('S&P 500')
plt.tight_layout()
sns.despine();


# %% codecell

signal = df_aapl['fChangeP'].fillna(0).squeeze()
fig, axes = plt.subplots(ncols=3, figsize=(14, 5))

wavelet = "db6"
for i, scale in enumerate([.1, .5, .75]):

    coefficients = pywt.wavedec(signal, wavelet, mode='per')
    coefficients[1:] = [pywt.threshold(i, value=scale*signal.max(), mode='soft') for i in coefficients[1:]]
    reconstructed_signal = pywt.waverec(coefficients, wavelet, mode='per')
    signal.plot(color="b", alpha=0.5, label='original signal', lw=2,
                 title=f'Threshold Scale: {scale:.1f}', ax=axes[i])

    pd.Series(reconstructed_signal, index=signal.index).plot(c='k', label='DWT smoothing}', linewidth=1, ax=axes[i])
    axes[i].legend()
fig.tight_layout()
sns.despine();


# %% codecell

# %% codecell

# %% codecell
dtc = DTCategoricalTransform(df_test)
stf = ScaleTransform(dtc.df)

bpath = Path(baseDir().path, 'ml_data', 'ml_training')
fpath = bpath.joinpath('_df_catkeys.parquet')

df_catkeys = pd.read_parquet(fpath)

dtc.df.info(verbose=True)

# %% codecell


# Fib start, fib high can both go, ext_end is probably the same





# %% codecell



# %% codecell











# %% codecell
