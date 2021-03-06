"""Testing daily routines."""
# %% codecell
####################################
import os.path
from datetime import date
import pandas as pd
from pathlib import Path
from collections import defaultdict

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate
    from api import serverAPI


# %% codecell
####################################
# IEX Close


# %% codecell
####################################

# %% codecell
####################################


class FpathsTest():
    """Check local fpaths for each process."""
    base_dir = baseDir().path
    sys_dict = {}

    def __init__(self):
        self.get_all_syms(self)
        self.check_iex_close(self)
        self.check_iex_stats(self)
        self.check_warrants(self)
        self.check_scans(self)
        self.check_cboe(self)
        self.check_stocktwits(self)
        self.check_nasdaq(self)
        self.check_bz_recs(self)
        self.check_yfinance_options(self)
        self.check_tdma(self)

    @classmethod
    def get_all_syms(cls, self):
        """Get the all_syms dataframe."""
        # all_syms = serverAPI('all_symbols').df
        syms_path = Path(baseDir().path, 'tickers', 'symbol_list', 'all_symbols.parquet')
        all_syms = pd.read_parquet(syms_path)
        sym_list = all_syms['symbol'].tolist()

        self.all_syms, self.sym_list = all_syms, sym_list

    @classmethod
    def check_iex_close(cls, self):
        """Check for iex_close and iex_combined."""
        dt = getDate.query('iex_eod')
        prev_dt = getDate.query('iex_previous')
        # IEX Close
        iex_close_fpath = f"{self.base_dir}/iex_eod_quotes/combined/_{dt}.parquet"

        prev_bpath = Path(self.base_dir, 'StockEOD')
        prev_comb = prev_bpath.joinpath('combined', f"_{prev_dt}.parquet")
        prev_comb_all = prev_bpath.joinpath('combined_all', f"_{prev_dt}.parquet")

        if prev_comb.exists():
            self.sys_dict['IEX Previous Daily Combined'] = True
        else:
            self.sys_dict['IEX Previous Daily Combined'] = False

        if prev_comb_all.exists():
            self.sys_dict['IEX Previous Daily Combined All'] = True
        else:
            self.sys_dict['IEX Previous Daily Combined All'] = False

        if os.path.isfile(iex_close_fpath):
            self.sys_dict['IEX daily stock data'] = True
            iex_close = pd.read_parquet(iex_close_fpath)
            iex_close_over = (iex_close[iex_close['symbol']
                              .isin(self.sym_list)]
                              .shape[0])
            iex_cov = round((iex_close_over / len(self.sym_list)) * 100, 2)

            self.sys_dict['IEX daily stock combined'] = True
            self.sys_dict['IEX close coverage'] = iex_cov

        else:
            self.sys_dict['IEX daily stock combined'] = False
            self.sys_dict['IEX close coverage'] = 0

    @classmethod
    def check_iex_stats(cls, self):
        """Check for iex company stats."""
        dt = getDate.query('iex_eod')
        fsuf = f"_{dt}.parquet"
        path = Path(baseDir().path, 'company_stats/stats/combined', fsuf)

        if path.exists():
            self.sys_dict['IEX company stats'] = True
        else:
            self.sys_dict['IEX company stats'] = False

    @classmethod
    def check_warrants(cls, self):
        """Check if local warrant files exist."""
        wt_path_base = f"{self.base_dir}/tickers/warrants"
        dt = getDate.query('iex_eod')

        wt_path_dict = ({
            'Warrants: cheapest': f"{wt_path_base}/cheapest/_{dt}.parquet",
            'Warrants: newest': f"{wt_path_base}/newest/_{dt}.parquet",
            'Warrants: top perf ytd': f"{wt_path_base}/top_perf/_{dt}.parquet",
            'Warrants: worst perf ytd': f"{wt_path_base}/worst_perf/_{dt}.parquet",
            'Warrants: all': f"{wt_path_base}/all/_{dt}.parquet",
            'Warrants: all historical': f"{wt_path_base}/all_hist/_{dt}.parquet"
        })

        for key in wt_path_dict.keys():
            if os.path.isfile(wt_path_dict[key]):
                self.sys_dict[key] = True
            else:
                self.sys_dict[key] = False

    @classmethod
    def check_scans(cls, self):
        """Check for local scans files."""
        dt = getDate.query('iex_eod')
        scans_path_base = f"{self.base_dir}/scans"

        scans_path_dict = ({
            'vol': {
                'avg': f"{scans_path_base}/top_vol/_{dt}.parquet"
            }
        })

        for keys in scans_path_dict.keys():
            for key in scans_path_dict[keys].keys():
                if os.path.isfile(scans_path_dict[keys][key]):
                    self.sys_dict[f"Scans: {keys}_{key}"] = True
                else:
                    self.sys_dict[f"Scans: {keys}_{key}"] = False

    @classmethod
    def check_cboe(cls, self):
        """Check local cboe file market maker opportunities."""
        dt = getDate.query('cboe')
        mkt_dt = getDate.query('mkt_open')
        mkt_yr = str(mkt_dt.year)
        cboe_bpath = f"{self.base_dir}/derivatives"
        cboe_syms_path = f"{cboe_bpath}/cboe/syms_to_explore"
        cboe_intra = f"{cboe_bpath}/cboe_intraday"

        cboe_path_dict = ({
            # 'cboe_raw': f"{cboe_base_path}/mmo/_{dt}.parquet",
            # 'CBOE nopop_2000': f"{cboe_bpath}/cboe/nopop_2000_{dt}.parquet",
            # 'CBOE long_time': f"{cboe_syms_path}/long_{dt}.parquet",
            # 'CBOE medium_time': f"{cboe_syms_path}/medium_{dt}.parquet",
            # 'CBOE short_time': f"{cboe_syms_path}/short_{dt}.parquet",
            'CBOE Intraday': f"{cboe_intra}/{mkt_yr}/_{mkt_dt}_eod.parquet"

        })

        cboe_parquet_dict = ({
            # 'cboe_raw': f"{cboe_base_path}/mmo/_{dt}.parquet",
            # 'CBOE nopop_2000': f"{cboe_bpath}/cboe/nopop_2000_{dt}.parquet",
            # 'CBOE long_time': f"{cboe_syms_path}/long_{dt}.parquet",
            # 'CBOE medium_time': f"{cboe_syms_path}/medium_{dt}.parquet",
            # 'CBOE short_time': f"{cboe_syms_path}/short_{dt}.parquet",
            'CBOE Intraday': f"{cboe_intra}/{mkt_yr}/_{mkt_dt}_eod.parquet"

        })

        for key in cboe_path_dict.keys():
            if os.path.isfile(cboe_path_dict[key]):
                self.sys_dict[key] = True
            elif os.path.isfile(cboe_parquet_dict[key]):
                self.sys_dict[key] = True
            else:
                self.sys_dict[key] = False

    @classmethod
    def check_stocktwits(cls, self):
        """Check stocktwits trending/user stream/me."""
        dt = date.today()
        st_base = f"{self.base_dir}/stocktwits"

        st_path_dict = ({
            'Stocktwits: trending': f"{st_base}/trending/{dt}.parquet",
            # 'Stocktwits: my watchlist': f"{st_base}/me/_watch_{dt}.parquet"

        })

        for key in st_path_dict.keys():
            if os.path.isfile(st_path_dict[key]):
                self.sys_dict[key] = True
            else:
                self.sys_dict[key] = False

    @classmethod
    def check_nasdaq(cls, self):
        """Check for nasdaq SSR list."""
        dt = getDate.query('iex_eod')
        bpath = Path(self.base_dir, 'short', 'daily_breaker')
        fpath = bpath.joinpath(f"nasdaq_{dt}.parquet")
        fpath_fmt = bpath.joinpath(f"nasdaq_{dt.strftime('%Y%m%d')}")

        if fpath.exists():
            self.sys_dict['Nasdaq: daily SSR list'] = True
        elif fpath_fmt.exists():
            self.sys_dict['Nasdaq: daily SSR list'] = True
        else:
            self.sys_dict['Nasdaq: daily SSR list'] = False

    @classmethod
    def check_bz_recs(cls, self):
        """Check for bz analyst recs."""
        dt = getDate.query('iex_close')
        fsuf = f"_{dt}.parquet"
        path = Path(baseDir().path, 'company_stats/analyst_recs', fsuf)

        if path.exists():
            self.sys_dict['Analyst ratings'] = True
        else:
            self.sys_dict['Analyst ratings'] = False

    @classmethod
    def check_yfinance_options(cls, self):
        """Check for combined yfinance options."""
        dt = getDate.query('iex_eod')
        fsuf = f"_{dt}.parquet"
        path = Path(baseDir().path, 'derivatives/end_of_day/combined', fsuf)

        if path.exists():
            self.sys_dict['Yoptions combined'] = True
        else:
            self.sys_dict['Yoptions combined'] = False

    @classmethod
    def check_tdma(cls, self):
        """Check TD Ameritrade fpaths."""
        dt = getDate.query('mkt_open')
        year = str(dt.year)
        fsuf = f"_{dt}.parquet"

        fdir_options = Path(baseDir().path, 'derivatives', 'tdma', 'series')
        # Fpath combined options
        fpath_comb = fdir_options.joinpath('combined', year, fsuf)
        # Fpath combined all options
        fpath_call = fdir_options.joinpath('combined_all', f"_{year}.parquet")

        if fpath_comb.exists():
            self.sys_dict['TDMA Options Combined'] = True
        else:
            self.sys_dict['TDMA Options Combined'] = False
