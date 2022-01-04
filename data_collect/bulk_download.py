"""Bulk download of yfinance historical data."""
# %% codecell

from pathlib import Path

import pandas as pd
import numpy as np
from tqdm import tqdm

import yfinance as yf

from api import serverAPI
from multiuse.help_class import getDate, baseDir, write_to_parquet
from multiuse.symbol_ref_funcs import get_symbol_stats
# %% codecell


class YfMaxHistorical():
    """Get 2015: max historical data."""

    """
    self.bpath : base_path
    self.df_all : combined_all for all files in bpath
    """

    def __init__(self, start="2015-01-01", end="2020-12-31"):
        sym_df, sym_start = self._find_missing_hist_symbols(self)
        data = self._get_yf_data(self, start, end, sym_start)
        self._write_to_local(self, data)
        self._combine_all(self)
        self._write_syms_to_years(self, self.df_all)

    @classmethod
    def _find_missing_hist_symbols(cls, self):
        """Finding all missing symbols from max historical."""
        bpath = Path(baseDir().path, 'historical/each_sym_all')
        self.bpath = bpath
        info_path = bpath.joinpath('info', 'info.parquet')
        
        if info_path.exists():
            sym_df = pd.read_parquet(info_path)
        else:
            df_stats = get_symbol_stats()
            symbols = df_stats['symbol'].dropna().unique().tolist()
            sym_df = pd.DataFrame(symbols, columns=['symbol'])

        sym_path_list = list(bpath.glob('**/*.parquet'))
        sym_list = [str(f).split('_')[-1].split('.')[0] for f in sym_path_list]

        sym_df['missing'] = np.where(sym_df['symbol'].isin(sym_list), 1, 0)

        write_to_parquet(sym_df, info_path)

        sym_missing = sym_df[sym_df['missing'] == 0]
        sym_start = sym_missing['symbol'].tolist()[0:1000]

        return sym_df, sym_start

    @classmethod
    def _get_yf_data(cls, self, start, end, sym_start):
        """Get YF data."""
        data = (yf.download(sym_start, start="2015-01-01",
                            end="2020-12-31", group_by="ticker"))
        return data

    @classmethod
    def _write_to_local(cls, self, data):
        """Write to local dataframes."""
        syms = data.columns.get_level_values(0).unique()
        bpath = Path(baseDir().path, 'historical/each_sym_all')

        for sym in tqdm(syms):
            df_sym = (data.loc[:, data.columns.get_level_values(0) == sym]
                          .droplevel(0, axis='columns')
                          .reset_index().copy())
            df_sym.insert(0, 'symbol', sym)
            df_sym.columns = [col.lower() for col in df_sym.columns]
            (df_sym.rename(columns={'Date': 'date', 'Open': 'fOpen',
                                    'High': 'fHigh', 'Low': 'fLow',
                                    'Close': 'fClose',
                                    'Volume': 'fVolume'}, inplace=True))

            sym_ea_path = bpath.joinpath(sym.lower()[0], f"_{sym}.parquet")
            write_to_parquet(df_sym, sym_ea_path)

    @classmethod
    def _combine_all(cls, self):
        """Combine all local files into a combined df."""
        bpath = Path(baseDir().path, 'historical/each_sym_all')
        sym_path_list = list(bpath.glob('**/*.parquet'))
        sym_path_list = ([f for f in sym_path_list
                          if 'info' not in str(f)
                          if 'combined_all' not in str(f)])

        sym_list = []

        for fpath in tqdm(sym_path_list):
            try:
                sym_list.append(pd.read_parquet(fpath))
            except Exception as e:
                print(str(e))

        f_suf = f"_{getDate.query('iex_eod')}.parquet"
        path_to_write = bpath.joinpath('each_sym_all', 'combined_all', f_suf)

        df_all = pd.concat(sym_list)
        df_all.columns = [col.lower() for col in df_all.columns]
        (df_all.rename(columns={'open': 'fOpen',
                                'high': 'fHigh', 'low': 'fLow',
                                'close': 'fClose',
                                'volume': 'fVolume'}, inplace=True))
        self.df_all = df_all
        write_to_parquet(df_all, path_to_write)

    @classmethod
    def _write_syms_to_years(cls, self, df_hist):
        """Take combined df and write to correct year, for each sym."""
        years = df_hist['date'].dt.year.unique()
        syms = df_hist['symbol'].unique()

        df_hist_idx = df_hist.copy()
        df_hist_idx['year'] = df_hist['date'].dt.year

        df_hist_idx = df_hist_idx.set_index(['symbol', 'year'])

        bpath = Path(baseDir().path, 'StockEOD')

        for sym in tqdm(syms):
            try:
                for yr in years:
                    df_mod = (df_hist_idx.loc[sym, yr]
                              .reset_index(level='symbol')
                              .reset_index(drop=True)
                              .copy())
                    yr_path = bpath.joinpath(str(yr), sym.lower()[0], f"_{sym}.parquet")

                    if yr_path.exists():
                        df_old = pd.read_parquet(yr_path)
                        df_all = pd.concat([df_old, df_mod])
                        df_all = df_all.drop_duplicates(subset=['date']).reset_index(drop=True)
                        write_to_parquet(df_all, yr_path)
                    else:
                        write_to_parquet(df_mod.reset_index(drop=True), yr_path)
            except Exception as e:
                print(sym)
                print(str(e))
                print()




# %% codecell

# %% codecell


# %% codecell


# %% codecell

# Separate by year

# data_melt.set_index('variable_0', inplace=True)


# %% codecell


# %% codecell




# %% codecell
