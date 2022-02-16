"""Make the full dataset to use for analysis."""
# %% codecell
from pathlib import Path
import sys
import importlib
from datetime import date

import pandas as pd
import numpy as np


try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, write_to_parquet
    from scripts.dev.workbooks_fib.fib_funcs_master import fib_master
    from scripts.dev.workbooks_fib.fib_funcs_clean_analysis import fib_all_clean_combine_write, add_fib_peaks_troughs_diffs, fib_pp_cleaned
    from scripts.dev.studies.ta_lib_studies import add_ta_lib_studies, make_emas
    from scripts.dev.workbooks_ml.ml_funcs_data_summary import create_summary_df_ml
    from scripts.dev.pre_process_data.scale_numericals import ScaleTransform
    from scripts.dev.pre_process_data.transform_dt_cat import DTCategoricalTransform
    from scripts.dev.pre_process_data.sec_daily_index import get_collect_prep_sec_data
    from scripts.dev.pre_process_data.pre_analyst_ratings import PreProcessAnalystRatings
    from scripts.dev.ref_data.symbol_meta_stats import SymbolRefMetaInfo
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, write_to_parquet
    from workbooks_fib.fib_funcs_master import fib_master
    from workbooks_fib.fib_funcs_clean_analysis import fib_all_clean_combine_write, add_fib_peaks_troughs_diffs, fib_pp_cleaned
    from studies.ta_lib_studies import add_ta_lib_studies, make_emas
    from workbooks_ml.ml_funcs_data_summary import create_summary_df_ml
    from pre_process_data.scale_numericals import ScaleTransform
    from pre_process_data.transform_dt_cat import DTCategoricalTransform
    from pre_process_data.sec_daily_index import get_collect_prep_sec_data
    from pre_process_data.pre_analyst_ratings import PreProcessAnalystRatings
    from ref_data.symbol_meta_stats import SymbolRefMetaInfo

importlib.reload(sys.modules['workbooks_fib.fib_funcs_master'])
importlib.reload(sys.modules['workbooks_fib.fib_funcs_clean_analysis'])
importlib.reload(sys.modules['studies.ta_lib_studies'])
importlib.reload(sys.modules['workbooks_ml.ml_funcs_data_summary'])
importlib.reload(sys.modules['pre_process_data.transform_dt_cat'])
# %% codecell

# Step #1 - run fib sequence
fib_master(verbose=False)

# path = Path(baseDir().path, 'studies/fibonacci', 'fib_vals.parquet')
# df = pd.read_parquet(path)

# %% codecell
dt = date(2022, 1, 1)
df_all = fib_all_clean_combine_write(dt=dt)

df_peak_troughs = add_fib_peaks_troughs_diffs(read=False).copy()
df_cleaned = fib_pp_cleaned(read=False, drop=False).copy()

df_sectors = SymbolRefMetaInfo(df_all=df_cleaned).df_all
df_studies = add_ta_lib_studies(df_sectors).copy()
df_studies = make_emas(df_studies).copy()
sec_df = get_collect_prep_sec_data(df=df_studies)

bpath = Path(baseDir().path, 'ml_data', 'ml_training')
path = bpath.joinpath('_fib_cleaned.parquet')
write_to_parquet(sec_df, path)

ppa_df = PreProcessAnalystRatings(df_all=sec_df).df


# %% codecell



# %% codecell
dtc = DTCategoricalTransform(ppa_df)


stf = ScaleTransform(dtc.df)

catkeys_fpath = bpath.joinpath('_df_catkeys.parquet')
df_catkeys = pd.read_parquet(catkeys_fpath)

df_processed = stf.df

process_fpath = bpath.joinpath('_df_processed.parquet')
write_to_parquet(df_processed, process_fpath)

# %% codecell

df_sum = create_summary_df_ml(df_processed[cols_to_use])

# %% codecell

fpath = '/Users/eddyt/Algo/data/ml_data/ml_training/_fib_cleaned_sum.csv'
df_drop = pd.read_csv(fpath)

df_drop.rename(columns={'Unnamed: 0': 'cols'}, inplace=True)
cols_to_use = df_processed.columns.difference(df_drop.cols.tolist())

df_sum = create_summary_df_ml(df_processed)

# %% codecell
