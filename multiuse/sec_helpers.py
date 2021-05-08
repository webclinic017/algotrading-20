"""Helper functions for sec routines."""
# %% codecell
##############################################
import datetime
import os.path
import glob

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate

# %% codecell
##############################################


def get_cik(sym):
    """Get SEC CIK number from symbol."""
    base_dir = baseDir().path
    all_syms_fpath = f"{base_dir}/tickers/all_symbols.gz"
    all_symbols = pd.read_json(all_syms_fpath, compression='gzip')

    # Drop cik values that are NaNs or infinite
    all_symbols.dropna(axis=0, subset=['cik'], inplace=True)
    all_symbols['cik'] = all_symbols['cik'].astype(np.uint32)

    cik = (all_symbols[all_symbols['symbol'] == sym]
           .head(1)['cik'].astype('uint32').iloc[0])
    return cik


def make_sec_cik_ref(sec_ref):
    """Make local dataframe with values from sec master."""
    base_dir, sec_ref_all = baseDir().path, pd.DataFrame()
    sec_ref_fpath = f"{base_dir}/tickers/sec_ref.gz"

    if os.path.isfile(sec_ref_fpath):
        sec_ref_old = pd.read_json(sec_ref_fpath, compression='gzip')
        sec_ref_all = pd.concat([sec_ref_old, sec_ref])
        sec_ref_all.drop_duplicates(subset=['CIK'], inplace=True)
        sec_ref_all.reset_index(drop=True, inplace=True)
    else:
        sec_ref_all = sec_ref.copy(deep=True)

    sec_ref_all.to_json(sec_ref_fpath, compression='gzip')


def sec_ref_from_combined():
    """Make local sec ref data from combined master_idx."""
    base_dir, mast_df = baseDir().path, None
    fpath_all = f"{base_dir}/sec/daily_index/_all_combined.gz"

    # Read sec_master_combined dataframe
    mast_df = pd.read_json(fpath_all, compression='gzip')
    mast_df.drop_duplicates(subset=['CIK'], inplace=True)

    sec_ref = mast_df[['CIK', 'Company Name']].copy(deep=True)
    sec_ref.reset_index(drop=True, inplace=True)

    # Define fpath of reference data
    base_dir = baseDir().path
    fpath = f"{base_dir}/tickers/sec_ref.gz"

    # Write reference data to local file
    sec_ref.to_json(fpath, compression='gzip')


def add_ciks_to_13FHRs():
    """Add cik column to existing 13FHRs."""
    base_dir = baseDir().path
    fpath = f"{base_dir}/sec/institutions/**/*.gz"
    choices = glob.glob(fpath, recursive=True)

    choice_dict = ({choice.split('_')[1].split('/')[0]:
                    choice for choice in choices})

    for key, path in choice_dict.items():
        df = pd.read_json(path, compression='gzip').copy(deep=True)
        df['CIK'] = key
        df.to_json(path, compression='gzip')


# %% codecell
##############################################


class secFpaths():
    """Return sec fpath."""

    base_sec = f"{baseDir().path}/sec"
    dt, fpath = False, False
    cat_choices = (['master_idx', 'company_idx',
                    'insider_trans', 'institutions'])

    def __init__(self, hist_date=False, cat=False, cik=False, sym=False):
        if not cat:
            print('You need to select a category')
            print(self.cat_choices)
            return
        if hist_date and cat == 'master_idx':
            self.master_idx_fpath(self, hist_date)
        if sym and not cik:
            cik = get_cik(sym)
        if cat == 'company_idx':
            self.company_idx_fpath(self, cik)
        elif cat == 'insider_trans':
            self.insider_trans_fpath(self, cik)
        # elif cat == 'institutions':
        #    self.institutions_fpath(self, cik)

    @classmethod
    def master_idx_fpath(cls, self, hist_date):
        """Determine parameters for fpaths to construct."""
        if not hist_date:
            dt = getDate.query('sec_master')
            dt_fmt, yr = dt.strftime("%Y%m%d"), dt.year
        elif isinstance(hist_date, datetime.date):
            dt_fmt = hist_date.strftime('%Y%m%d')
            yr = hist_date.yr
        elif len(hist_date) == 8:
            yr = hist_date[0:4]
            dt_fmt = hist_date

        self.fpath = f"{self.base_sec}/daily_index/{yr}/_{dt_fmt}.gz"

    @classmethod
    def company_idx_fpath(cls, self, cik):
        """Get index of company filings."""
        self.fpath = f"{self.base_sec}/company_index/{str(cik)[-1]}/_{cik}.gz"

    @classmethod
    def insider_trans_fpath(cls, self, cik):
        """Get index of company filings."""
        self.fpath = f"{self.base_sec}/insider_trans/{str(cik)[-1]}/_{cik}.gz",

    @classmethod
    def institutions_fpath(cls, self, cik):
        """Get insitutional filings by financial quarter."""

        """
        fpath_quart = f"{fpath_base}/{f_cik[-1]}/_{f_cik}/{f_quart}"
        f_quart = f"Q{str((dt.month - 1) // 3 + 1)}"
        yr = dt.year
        self.fpath = f"{self.base_sec}/institutions/{yr}/{str(cik)[-1]}/_{cik}/{f_quart}/_.gz"
        f_quart = f"Q{str((row_dt.month - 1) // 3 + 1)}"
        """
