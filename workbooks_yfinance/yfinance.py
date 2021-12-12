"""Yfinance options analysis."""
# %% codecell

import importlib
import sys
import os
from socks import SOCKS5AuthError
from dotenv import load_dotenv
from tqdm import tqdm
from io import StringIO, BytesIO
import string

import yfinance as yf
import pandas as pd
import dask.dataframe as dd
import requests
from tqdm import tqdm
from pathlib import Path
from datetime import date

import matplotlib.pyplot as plt

from api import serverAPI

from master_funcs.master_iex_stats import MasterIexStats

from multiuse.help_class import baseDir, getDate, df_create_bins, help_print_arg, dataTypes, help_print_error, check_size, write_to_parquet
from multiuse.symbol_ref_funcs import get_all_symbol_ref
from multiuse.api_helpers import get_sock5_nord_proxies

from data_collect.yfinance_funcs import clean_yfinance_options, get_cboe_ref, return_yoptions_temp_all, yoptions_combine_last
from data_collect.iex_class import urlData, get_options_symbols
from data_collect.sec_rss import SecRssFeed, AnalyzeSecRss
from data_collect.reference_data import IntSyms

from workbooks_yfinance.merge_yoptions_hist import analyze_iex_ytd, get_clean_yoptions, get_clean_all_st_data


importlib.reload(sys.modules['multiuse.help_class'])

importlib.reload(sys.modules['data_collect.iex_class'])

importlib.reload(sys.modules['data_collect.yfinance_funcs'])


importlib.reload(sys.modules['api'])


importlib.reload(sys.modules['master_funcs.master_iex_stats'])

importlib.reload(sys.modules['data_collect.sec_rss'])


# %% codecell
##########################################
serverAPI('redo', val='clear_yoptions_temp_unfin')
# serverAPI('redo', val='make_yoptions_file_struct')
# %% codecell
serverAPI('redo', val='master_yfinance_options_collect')
# %% codecell
serverAPI('redo', val='master_yfinance_options_followup')
# %% codecell
serverAPI('redo', val='yoptions_combine_last')
# %% codecell
serverAPI('redo', val='combine_yoptions_combine_all')
# %% codecell
serverAPI('redo', val='combine_yoptions_all')
# %% codecell
serverAPI('redo', val='yoptions_combine_temp_all')
# %% codecell
serverAPI('redo', val='yoptions_drop_hist_dupes')
# %% codecell
serverAPI('redo', val='split_iex_hist_last_month')
# %% codecell
serverAPI('redo', val='nasdaq_ssr_list')
# %% codecell
serverAPI('redo', val='split_iex_hist_previous')
# %% codecell
serverAPI('redo', val='combine_daily_stock_eod')
# %% codecell
serverAPI('redo', val='master_iex_stats')
# %% codecell
serverAPI('redo', val='combine_stats')
# %% codecell
serverAPI('redo', val='sec_rss_task')
# %% codecell
serverAPI('redo', val='cboe_close')
# %% codecell
serverAPI('redo', val='gz_to_parq_dirs')
# %% codecell
serverAPI('redo', val='gz_to_parquet')
# %% codecell
serverAPI('redo', val='get_sizes')
# %% codecell
df_sizes = serverAPI('gz_file_sizes').df
# %% codecell
serverAPI('redo', val='syms_to_parq')
# %% codecell
serverAPI('redo', val='GetMissingDates')
# %% codecell
serverAPI('redo', val='warrants')
# %% codecell
serverAPI('redo', val='get_missing_hist_from_yf')
# %% codecell
serverAPI('redo', val='CboeIntraday')
# %% codecell
serverAPI('redo', val='combine_all_cboe_symref')
# %% codecell

mf_url = '/ref-data/mutual-funds/symbols'
mf_syms = urlData(mf_url).df

path = Path(baseDir().path, 'tickers', 'mfund_symbols.parquet')
mf_syms.info()

# %% codecell

yall_today = serverAPI('yoptions_daily').df
# yall_all = serverAPI('yoptions_all').df
yall_dd = dd.from_pandas(yall_today, npartitions=1)

cboe_symref = serverAPI('cboe_symref_all').df
cboe_dd = dd.from_pandas(cboe_symref, npartitions=1)

cboe_dd['OSI Symbol'] = cboe_dd['OSI Symbol'].str.replace(' ', '')
cboe_dd['expirationDate'] = cboe_dd['expirationDate'].astype('int64')
cboe_dd['expirationDate'] = dd.to_datetime(cboe_dd['expirationDate'], format='%y%m%d')

yall_dd['contractSymbol'] = yall_dd['contractSymbol'].str.strip(string.whitespace)
yall_dd['contractSymbol'] = yall_dd['contractSymbol'].str.replace(' ', '')
dd_all = dd.merge(yall_dd, cboe_dd, how='left', left_on=['contractSymbol'], right_on=['OSI Symbol'], indicator=True)

# %% codecell
"""
dd_all['sym_suf'] = dd_all.apply(lambda row: row['contractSymbol'].split(row['symbol']), axis=1, meta=('sym_suf', 'object'))
dd_all['sym_suf'] = dd_all.apply(lambda row: row['sym_suf'][1], axis=1, meta=('sym_suf', 'object'))

dd_all['sym_suf_mod'] = dd_all.apply(lambda row: row['sym_suf'].replace('C', ' ').replace('P', ' ').split(' '), axis=1, meta=('sym_suf_mod', 'object'))
dd_all['expDateTest'] = dd_all.apply(lambda row: row['sym_suf_mod'][0], axis=1, meta=('expDateTest', 'object'))
dd_all['expDateTest'] = dd.to_datetime(dd_all['expDateTest'], format='%y%m%d')
dd_all['strikeTest'] = dd_all.apply(lambda row: row['sym_suf_mod'][1], axis=1, meta=('strikeTest', 'object'))
dd_all['strikeTest'] = dd_all.apply(lambda row: f"{row['strikeTest'][:5]}.{row['strikeTest'][-3:]}", axis=1, meta=('strikeTest', 'object'))
dd_all['strikeTest'] = dd.to_numeric(dd_all['strikeTest'])
"""
# %% codecell
dd_all.head()

ocgn_df = dd_all[dd_all['Underlying'] == 'OCGN'].compute()
# cboe_dd = cboe_dd.rename(columns={'OSI Symbol': 'contractSymbol'})

# %% codecell

fig, ax = plt.subplots()

for key in ocgn_df.groupby(['expDate']):
    ax = ocgn_df.plot(ax=ax, kind='line', x='date', y='impliedVolatility', label=key)

plt.legend(loc='best')
plt.show()

import seaborn as sns

# %% codecell

for key in ocgn_df.groupby(['expDate']):
    sns.lineplot(data=ocgn_df, x='date', y='impliedVolatility', label=key)

# %% codecell
ocgn_dd.head()

ocgn_df['expDate'] = pd.to_datetime(ocgn_df['contractSymbol'].str[4:10], format='%y%m%d')
ocgn_sub = ocgn_df[['expDate', 'date', 'impliedVolatility']].copy()
ocgn_sub.drop_duplicates(subset=['expDate', 'date'], inplace=True)
ocgn_sub.dropna(inplace=True)
ocgn_sub.reset_index(drop=True, inplace=True)
ocgn_sub
ocgn_plot = ocgn_sub.pivot("date", "expDate", "impliedVolatility")
ocgn_plot
sns.lineplot(data=ocgn_sub, x='date', y=ocgn_sub.columns)

# %% codecell

sapi = serverAPI('yoptions_stock', symbol='OCGN')


sapi.df
# %% codecell




# %% codecell


ocgn_df['sym_suf'] = ocgn_df.apply(lambda row: row['contractSymbol'].split(row['symbol']), axis=1)
ocgn_df.apply(lambda row: row['sym_suf'][1], axis=1)
ocgn_df

ocgn_df['_merge'].value_counts()
df_sub = ocgn_all_df[['expDate', 'date', 'impliedVolatility']]

ocgn_all_df.info()



df_sub.reset_index().drop_duplicates(subset=['date', 'expDate'])

df_sub.set_index(['date', 'expDate'])

df_sub.sort_values(by=['date'], ascending=True).set_index('date')

df_sub.set_index(['date', 'expDate'])

ocgn_all_df[['expDate', 'date', 'impliedVolatility']].set_index('date').pivot(columns='expDate', values='impliedVolatility')

ocgn_all_df[['expDate', 'date', 'impliedVolatility']].set_index(['expDate']).unstack()


ocgn_all_df[['expDate', 'date', 'impliedVolatility']].plot(x='date', y='impliedVolatility', stacked=True)

ocgn_all_df['expDate'].value_counts()

ocgn_all_df.info()
ocgn_df.drop_duplicates(subset=['contractSymbol', 'lastTradeDate'])


ocgn_df.head()

ocgn_df.info()
ocgn_df['expDate'] = ocgn_df['contractSymbol'].str[3:10].str.strip(string.ascii_letters)

ocgn_df['expDate'].value_counts()

yall_today['expDate'].value_counts()

yall_today[yall_today['symbol'] == 'OCGN'].shape

ocgn_df['date'].value_counts()
ocgn_df['expDate'].value_counts()

ocgn_df[['date', 'impliedVolatility']].groupby(by='date').mean().plot()



# %% codecell

bpath = Path(baseDir().path, 'derivatives/end_of_day/combined')
f_suf = f"_{getDate.query('iex_eod')}.parquet"
bpath.parent.joinpath('combined_all', f_suf)

serverAPI('redo', val='combine_yoptions_combine_all')
# %% codecell

# %% codecell


# %% codecell


# %% codecell
# Okay, so my original motivation here was to get a specific day for OCGN
# Why don't I request that data again and see if it's available, because if this all worked then it should be
null_df = pd.read_parquet('/Users/eddyt/Algo/data/StockEOD/missing_dates/null_dates/_null_dates.parquet')
null_df.info()


df = (pd.merge(df, df_null, on=['date', 'symbol'],
               how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop(columns='_merge', axis=1)
        .reset_index(drop=True))

# Okay so why doesn't there exist a combined_all folder with combined stock_eods

stock_all = serverAPI('stock_close_prices')
df_all = stock_all.df.copy()
cols_to_keep = (['symbol', 'date', 'fClose', 'fOpen', 'fHigh', 'fLow',
                 'fVolume', 'changeOverTime', 'change', 'changePercent'])
df_all = df_all[cols_to_keep].copy()

df_ocgn = df_all[df_all['symbol'] == 'OCGN']

df_ocgn

# %% codecell
stock_cb_all = serverAPI('stock_close_cb_all')

# %% codecell
cb_all = stock_cb_all.df

cb_all[cb_all['symbol'] == 'OCGN']['date']

df_cb = cb_all[cols_to_keep].copy()
df_cb.info()
df_cb['date'] = pd.to_datetime(df_cb['date'])
df_cb[df_cb['symbol'] == 'OCGN']['fHigh'].max()



df = df_cb[['date', 'symbol']]
# %% codecell
df = (pd.merge(df, null_df, on=['date', 'symbol'],
               how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop(columns='_merge', axis=1)
        .reset_index(drop=True))

# %% codecell
df.info()

# %% codecell
# Okay this STILL isn't a full data set.
# Only thing I can come up with here is to just combine everything
# First let me check the fpath errors for missing dates

# Oh also, printing random errors is super unhelpful. Make much more sense to
# write the errors to local files and then deal with them

# How is this still not fucking working. I don't understand what the issue is anymore.
# Like this shouldn't even be a problem at this point!

# One thing I can do to reproduce the error is write all these files locally, then dissect them.
# That's not the worst idea. However, instead of 25,000, maybe I can just do the cs ~ 5,500

all_syms = serverAPI('all_symbols').df
all_cs = all_syms[all_syms['type'] == 'cs'].reset_index(drop=True)

cb_cs = cb_all[cb_all['symbol'].isin(all_cs['symbol'].tolist())]



for sym in tqdm(cb_cs['symbol'].unique()):
    df_mod = cb_cs[cb_cs['symbol'] == sym].reset_index(drop=True)
    fpath = Path(baseDir().path, 'StockEOD/2021', sym.lower()[0], f"_{sym}.parquet")
    write_to_parquet(df_mod, fpath)


cb_all.info()
ocgn_df = df_cb[df_cb['symbol'] == 'OCGN']

ocgn_df['fClose'].max()
ocgn_df[ocgn_df['fHigh'] == ocgn_df['fHigh'].max()]


cgn_df['date'].unique()
# %% codecell


# %% codecell
# Test the internal APIs
api_options = serverAPI.url_dict

symref = serverAPI('cboe_symref')
symref_df = symref.df
symref_df.info()
api_options

# %% codecell
sapi_null = serverAPI('missing_dates_null')
df_null = sapi_null.df

df_null.info()

# %% codecell
sapi_less = serverAPI('missing_dates_less')
df_less = sapi_less.df

df_less.reset_index(drop=True).info()
# %% codecell


# %% codecell
sapi_mall = serverAPI('missing_dates_all')
df_mall = sapi_mall.df

df_mall.info()
# %% codecell

df_mall[df_mall['symbol'] == 'OCGN']


# %% codecell

# Okay so what now? I can start working
# Through the analyzing news articles chapter - if I can even
# Remember which one that was

# I still don't have a news stream but honestly with a few hours of
# research I can figure that out. That part will probably
# be easier than using AI to analyze all this shit.

# %% codecell

# Preferable to the current ref data collection I have
# local_cs_path = Path(bpath, 'iex_intra_prices.parquet')
# local_otc_path = Path(bpath, 'iex_otc_syms.parquet')

# %% codecell
# str(int_syms.__class__).split('.')[-1].split("'")[0]
# Name of class = type(tt).__name__
# or print(type(self).__name__)


# %% codecell

# %% codecell

df_2021 = analyze_iex_ytd()
df_cleaned = get_clean_yoptions()

df_prices = df_2021.copy()


# %% codecell
perc_path = Path(baseDir().path, 'StockEOD/combined',
                 "_2021_yprices_percs.parquet")
df_y = pd.read_parquet(perc_path)

# %% codecell
st_all = get_clean_all_st_data()
# %% codecell

# %% codecell


# %% codecell

df_merge = pd.merge(df_cleaned, df_y.reset_index(),
                    on=['date', 'symbol'], how='inner')
df_merge.drop_duplicates(subset=['contractSymbol', 'date'], inplace=True)

df_merged = pd.merge(df_merge, st_all, on=['date', 'symbol'], how='left')
df_merged.drop_duplicates(subset=['contractSymbol', 'date'], inplace=True)
# %% codecell
merged_fpath = Path(baseDir().path, 'StockEOD/combined',
                    "_2021_merged_all.parquet")
df_merged = dataTypes(df_merged, parquet=True).df
df_merged.to_parquet(merged_fpath)
# %% codecell

df_merged = pd.read_parquet(merged_fpath)
df_merged = df_merged.reset_index(drop=True)
# %% codecell
df_merged['inTheMoney'].value_counts()

df_merged.info()

# I'd like to find the ratio of in the money calls to out of the money calls
cols_to_group = ['symbol', 'date', 'expDate',
                 'inTheMoney', 'side', 'volume', 'openInterest']
df_pre_groupby = df_merged[cols_to_group].copy()

df_grouped = df_pre_groupby.groupby(
    by=['symbol', 'date', 'expDate', 'side', 'inTheMoney'], as_index=False).sum()
df_group_na = df_grouped.dropna().reset_index(drop=True)
dict_to_rename = {'volume': 'groupVol', 'openInterest': 'groupOI'}
df_group_na.rename(columns=dict_to_rename, inplace=True)
# %% codecell


df_mgroup = pd.merge(df_merged, df_group_na, on=[
                     'symbol', 'date', 'expDate', 'side', 'inTheMoney'])
df_mgroup['groupVol/OI'] = (
    df_mgroup['groupVol'].div(df_mgroup['groupOI'])).round(1)

df_mgroup.drop_duplicates(subset=['contractSymbol', 'date'], inplace=True)
df_mgroup.drop_duplicates(
    subset=['date', 'symbol', 'expDate', 'strike', 'side'], inplace=True)
df_mgroup.info()
df_grouped.info()
df_grouped.head()
df_group_na.info()

df_group_na.head()
df_mgroup.head()
# %% codecell


cleaned_to_show = (['Underlying', 'count', 'date', 'expDate', 'side', 'strike', 'vol/oi',
                    'volume', 'openInterest', 'premium', 'inTheMoney', 'c_perc1',
                    'c_perc2', 'c_perc3', 'c_perc5', 'c_perc7'])


df_merged[(df_merged['volume'] > 100) & (df_merged['side'] == 'P')].sort_values(
    by=['c_perc7', 'vol/oi'], ascending=True).head(50)[cleaned_to_show]

sort_by = ['count']

(df_merged[(df_merged['volume'] > 100)
           & (df_merged['side'] == 'C')
           & (df_merged['openInterest'] > 1)].sort_values(by=sort_by, ascending=False)
 .head(50)[cleaned_to_show])


# %% codecell

grouped_to_show = (['Underlying', 'count', 'date', 'expDate', 'side', 'groupVol', 'groupOI', 'groupVol/OI', 'inTheMoney',
                    'c_perc1', 'c_perc2', 'c_perc3', 'c_perc5', 'c_perc7'])


sort_by = ['groupVol/OI']
df_analyze = (df_mgroup[(df_mgroup['volume'] > 100) &
                        (df_mgroup['side'] == 'C')
                        & (df_mgroup['inTheMoney'] == 1)
                        & (df_mgroup['openInterest'] > 1)].sort_values(by=sort_by, ascending=False)
              .dropna()
              .head(50)[grouped_to_show])

df_analyze.head(50)

# %% codecell
all_syms = serverAPI('all_symbols').df

all_cs = all_syms[all_syms['type'] == 'cs'].reset_index(drop=True)
all_cs.info()
# %% codecell
df_mcs = (df_merged[df_merged['symbol'].isin(all_cs['symbol'].tolist())]
          .copy())

# % codecell
sort_by = ['vol/oi']
df_acs = (df_mcs[(df_mcs['volume'] > 100)
                 & (df_mcs['side'] == 'C')
                 & (df_mcs['openInterest'] > 0)].sort_values(by=sort_by, ascending=False)
          .head(50)[cleaned_to_show])

df_acs.head(50)

# %% codecell
ref_df['expDatesTest'] = ref_df.apply(
    lambda row: row.expDates[:(len(row.expDates) // 2)], axis=1)

ref_df['minus'] = ref_df.apply(lambda row: list(
    set(row.expDates) - set(row.expDatesTest)), axis=1)

# %% codecell

# Get social sentiment data


# %% codecell

df_stats = serverAPI('stats_combined').df

df_stats.info()
# %% codecell

# I'd now like to see if there was a signficant company event

# %% codecell

df_stats['nextEarningsDate'].head()

# It'd be nice to pull a dataframe for each day of all earnings, utilizing this method
cols_to_view = ['symbol', 'nextEarningsDate']


df_stats[]


# %% codecell

# %% codecell


# %% codecell


# %% codecell
st_all.info()

# %% codecell

# %% codecell

# %% codecell

# %% codecell


# %% codecell

df_unfin = serverAPI('yoptions_unfin').df
df_temp = serverAPI('yoptions_all').df

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell


# %% codecell
# %% codecell


# %% codecell

cleaned_to_show = ['Underlying', 'expDate', 'side', 'strike', 'vol/oi', 'volume',
                   'openInterest', 'premium', 'bid', 'mid', 'ask', 'percentChange', 'lastTradeDay']
cols_to_show = ['symbol', 'expDate', 'side', 'vol/avg', 'Cboe ADV',
                'volume', 'totVol', 'openInterest', 'impliedVolatility']

df_cleaned.info()

cleaned_to_show = ['Underlying', 'expDate', 'side', 'strike', 'vol/oi', 'volume',
                   'openInterest', 'premium', 'bid', 'mid', 'ask', 'percentChange', 'lastTradeDay']
df_cleaned.sort_values(
    by=['vol/oi'], ascending=False).head(50)[cleaned_to_show]


df_today = df_cleaned[df_cleaned['lastTradeDate'].dt.date
                      == getDate.query('cboe')].copy()

# %% codecell

# %% codecell


# %% codecell


df_pc = df_today.groupby(by=['symbol', 'expDate', 'side']).sum()[
                         'volume'].dropna().to_frame()
df_pc
df_pc
df_pc.unstack('side')
df_pc.reset_index()
df_pc.index.get_level_values('volume')
df_pc.loc[:, 'p/c'] = df_pc['P'].div(df_pc['C']).round(2)

df_today.shape
df_today.sort_values(by=['vol/oi'], ascending=False).head(50)[cleaned_to_show]

# %% codecell

cboe_last = serverAPI('cboe_dump_last').df

cboe_last.info()

# %% codecell
cols_to_drop = ['strike_x']
cboe_last.drop(columns=cols_to_drop, inplace=True)
cboe_last['expDate'] = pd.to_datetime(cboe_last['expDate'], format='%Y-%m-%d')
cboe_last['dataDate'] = pd.to_datetime(cboe_last['dataDate'], unit='ms')

cboe_last.rename(columns={'dataDate': 'date',
                 'Underlying': 'symbol'}, inplace=True)
cboe_last.head()

# %% codecell
df_comb = pd.merge(cboe_last, df_cleaned, on=[
                   'expDate', 'symbol', 'side', 'date'])

df_comb.columns
df_comb.head()
# %% codecell

df_larger_avg_vol = df_comb[df_comb['Cboe ADV'] > 1].copy()
df_larger_avg_vol.columns
# %% codecell
df_cleaned.shape

sym = 'RKLB'
ticker = yf.Ticker(sym)
sym_ops = ticker.option_chain()
df_puts = sym_ops.puts
df_calls = sym_ops.calls

df_sym = pd.concat([df_puts, df_calls])
df_sym[df_sym['strike'] == 1].head()

# %% codecell
cols_to_show = ['symbol', 'expDate', 'side', 'vol/avg', 'Cboe ADV',
                'volume', 'totVol', 'openInterest', 'impliedVolatility']

df_larger_avg_vol.sort_values(
    by=['vol/avg'], ascending=False).head(50)[cols_to_show]


# %% codecell


# %% codecell
try:
    df_cleaned = clean_yfinance_options()
except Exception as e:
    print(str(e))

df_cleaned.info()
# %% codecell

df_temp_all = return_yoptions_temp_all()
df_temp_all = serverAPI('yoptions_temp').df


len(df_cleaned['symbol'].unique())
# %% codecell

# Replace open interests of 0 with 1 instead

# %% codecell

# %% codecell
cleaned_to_show = ['symbol', 'expDate', 'side', 'strike', 'vol/oi',
                   'volume', 'openInterest', 'premium', 'bid', 'mid', 'ask', 'percentChange']

# %% codecell
df_cleaned['mid'].value_counts()
# %% codecell
df_cleaned.sort_values(by=['premium'], ascending=False).head(50)[
                       cleaned_to_show]

df_cleaned.sort_values(by=['openInterest'], ascending=False).head(50)[
                       cleaned_to_show]

# Takeaways:
# 1. Calls/puts are sometimes bought when stock hits major support/resistance level

df_cleaned.sort_values(
    by=['vol/oi'], ascending=False).head(50)[cleaned_to_show]

# %% codecell
for col in df_comb.columns:
    print(f"column {col} {df_comb[col].isna().sum()}")


# %% codecell

# %% codecell
df_all = serverAPI('yoptions_all').df
df_cleaned1 = clean_yfinance_options(df_temp=df_all, refresh=True)

# %% codecell

df_cleaned1.columns

df_all.info()

df_all.info()

# %% codecell

# %% codecell

# %% codecell

# At the share price market offer - or above the share price market offering

df_cleaned1.sort_values(
    by=['vol/oi'], ascending=False).head(50)[cleaned_to_show]


# %% codecell


# %% codecell
df_all.info()


# %% codecell

# df_comb['expDatesLength'] = df_comb.apply(lambda row: len(row.expDatesNeeded), axis=1)
proxies = get_sock5_nord_proxies()


# %% codecell

ref_path = Path(baseDir().path, 'ref_data', 'syms_with_options.parquet')
ref_df = pd.read_parquet(ref_path)

ref_df.head()
# %% codecell


sym_test = 'OCGN'
sym_list = [sym_test] * 10

exp_test_list = ref_df['expDates'][0][0:10]

test_df = pd.DataFrame()
test_df['symbol'] = sym_list
test_df['expDate'] = exp_test_list
test_df['expDates'] = 0

grouped = (test_df.groupby(by=['symbol'])['expDate']
                  .agg({lambda x: list(x)})
                  .reset_index()
                  .rename(columns={'<lambda>': 'expDates'}))


dt = getDate.query('iex_eod')
yr, fpath = dt.year, ''
fpath_base = Path(baseDir().path, 'derivatives/end_of_day', 'temp', '2021')
fpaths = list(fpath_base.glob('**/*.parquet'))
len(fpaths)
test_df.head(10)

# %% codecell


# %% codecell

SetUpYahooOptions(followup=False, testing=True)


# os.path.getsize(path)
path = Path(baseDir().path, 'derivatives/end_of_day/unfinished', 'empty.parquet')
ref_df[ref_df['symbol'] == 'AAAAAAAAAAA'].copy().to_parquet(path)


# %% codecell


# %% codecell


# %% codecell


# %% codecell


# %% codecell


# %% codecell

nord_url = 'https://nordvpn.com/api/server'
get = requests.get(nord_url)

server_list = pd.DataFrame(get.json())
col_list = list(server_list['features'].iloc[0].keys())
server_list[col_list] = server_list.features.apply(lambda row: pd.Series(row))
socks_df = server_list[server_list['socks'] == True].copy()

nord_pass = 'YNGWqBf2zHaLSV6'
purl = f"edwardtomasso@gmail.com:{nord_pass}@"
socks_df['socksp'] = socks_df.apply(lambda row:
                                    {'http': f"socks5://{purl}{row.domain}:1080",
                                     'https': f"socks5://{purl}{row.domain}:1080"},
                                    axis=1)


nord_df['p_test'].iloc[0]

nord_df['p_test'].iloc[10]
# %% codecell
int(sym_list.shape[0] / (socks_df.shape[0] - 1))


# %% codecell


# %% codecell

# %% codecell


# %% codecell
##########################################


# %% codecell


# %% codecell

# %% codecell
