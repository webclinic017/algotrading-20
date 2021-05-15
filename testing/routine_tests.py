"""Testing daily routines."""
# %% codecell
####################################
import os.path
from datetime import date
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate


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
        self.check_warrants(self)
        self.check_scans(self)
        self.check_cboe(self)
        self.check_stocktwits(self)

    @classmethod
    def get_all_syms(cls, self):
        """Get the all_syms dataframe."""
        syms_fpath = f"{self.base_dir}/tickers/all_symbols.gz"
        all_syms = pd.read_json(syms_fpath, compression='gzip')
        sym_list = all_syms['symbol'].tolist()

        self.all_syms, self.sym_list = all_syms, sym_list

    @classmethod
    def check_iex_close(cls, self):
        """Check for iex_close and iex_combined."""
        dt = getDate.query('iex_close')
        # IEX Close
        iex_close_fpath = f"{self.base_dir}/iex_eod_quotes/combined/_{dt}.gz"

        if os.path.isfile(iex_close_fpath):
            self.sys_dict['iex_close_combined'] = True
            iex_close = pd.read_json(iex_close_fpath, compression='gzip')
            iex_close_len = len(self.sym_list)
            iex_close_over = (iex_close[iex_close['symbol']
                              .isin(self.sym_list)]
                              .shape[0])
            iex_close_cov = round((iex_close_over / iex_close_len) * 100, 2)

            self.sys_dict['iex_close_combined'] = True
            self.sys_dict['iex_close_cov'] = iex_close_cov

        else:
            self.sys_dict['iex_close_combined'] = False
            self.sys_dict['iex_close_cov'] = 0

    @classmethod
    def check_warrants(cls, self):
        """Check if local warrant files exist."""
        wt_path_base = f"{self.base_dir}/tickers/warrants"
        dt = getDate.query('iex_close')

        wt_path_dict = ({
            'cheapest': f"{wt_path_base}/cheapest/_{dt}.gz",
            'newest': f"{wt_path_base}/newest/_{dt}.gz",
            'top_perf': f"{wt_path_base}/top_perf/_{dt}.gz",
            'worst_perf': f"{wt_path_base}/worst_perf/_{dt}.gz",
            'all': f"{wt_path_base}/all/_{dt}.gz",
            'all_hist': f"{wt_path_base}/all_hist/_{dt}.gz"
        })

        for key in wt_path_dict.keys():
            if os.path.isfile(wt_path_dict[key]):
                self.sys_dict[key] = True
            else:
                self.sys_dict[key] = False

    @classmethod
    def check_scans(cls, self):
        """Check for local scans files."""
        dt = getDate.query('iex_close')
        scans_path_base = f"{self.base_dir}/scans"

        scans_path_dict = ({
            'vol': {
                'avg': f"{scans_path_base}/top_vol/_{dt}.gz"
            }
        })

        for keys in scans_path_dict.keys():
            for key in scans_path_dict[keys].keys():
                if os.path.isfile(scans_path_dict[keys][key]):
                    self.sys_dict[f"scans_{keys}_{key}"] = True
                else:
                    self.sys_dict[f"scans_{keys}_{key}"] = False

    @classmethod
    def check_cboe(cls, self):
        """Check local cboe file market maker opportunities."""
        dt = getDate.query('cboe')
        cboe_base_path = f"{self.base_dir}/derivatives"
        cboe_syms_path = f"{cboe_base_path}/cboe/syms_to_explore"

        cboe_path_dict = ({
            'cboe_raw': f"{cboe_base_path}/mmo/_{dt}.gz",
            'cboe_nopop_2000': f"{cboe_base_path}/cboe/nopop_2000_{dt}.gz",
            'cboe_long_time': f"{cboe_syms_path}/long_{dt}.gz",
            'cboe_medium_time': f"{cboe_syms_path}/medium_{dt}.gz",
            'cboe_short_time': f"{cboe_syms_path}/short_{dt}.gz"

        })

        for key in cboe_path_dict.keys():
            if os.path.isfile(cboe_path_dict[key]):
                self.sys_dict[key] = True
            else:
                self.sys_dict[key] = False

    @classmethod
    def check_stocktwits(cls, self):
        """Check stocktwits trending/user stream/me."""
        dt = date.today()
        st_base = f"{self.base_dir}/stocktwits"

        st_path_dict = ({
            'st_trending': f"{st_base}/trending/{dt}.gz",
            'st_my_watchlist': f"{st_base}/me/_watch_{dt}.gz"

        })

        for key in st_path_dict.keys():
            if os.path.isfile(st_path_dict[key]):
                self.sys_dict[key] = True
            else:
                self.sys_dict[key] = False
