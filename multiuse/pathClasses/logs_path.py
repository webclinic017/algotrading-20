"""Fpaths for local logs."""
# %% codecell

from pathlib import Path
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

# %% codecell


class LogsPaths():
    """Paths and dataframes for logs."""

    bpath = Path(baseDir().path, 'logs')

    def __init__(self, log=None, **kwargs):
        if log:
            valid = self._validate_log_param(self, log, **kwargs)
            if valid:
                self.fpath = self._construct_fpath(self, log, **kwargs)
                self.df = self._get_df(self, self.fpath, **kwargs)
        else:
            self.get_lognames()

    @staticmethod
    def get_lognames():
        """Get a list of all available logname stems with parents."""
        bpath = Path(baseDir().path, 'logs')
        f_list = list(bpath.rglob('[!.DS_Store]*'))
        log_list = []
        for f in f_list:
            if not f.is_dir():
                log_list.append(f"{f.parent.name}/{f.name}")
        return log_list

    @classmethod
    def _validate_log_param(cls, self, log, **kwargs):
        """Validate log param against existing values."""
        dirslist = list(self.bpath.glob('[!.]*'))
        stem_list = [f.stem for f in dirslist]
        stemname = log.split('/')[0]

        if stemname in stem_list:
            return True
        else:
            return False

    @classmethod
    def _construct_fpath(cls, self, log, **kwargs):
        """Construct fpath."""
        # Default name is api_calls. F
        fpath = self.bpath.joinpath(log)
        return fpath

    @classmethod
    def _get_df(cls, self, fpath, **kwargs):
        """Get and return df, if it exists."""
        df = pd.DataFrame()
        if fpath.exists():
            if '.parquet' in fpath.name:
                df = pd.read_parquet(fpath)
            elif '.pickle' in fpath.name:
                df = pd.read_pickle(fpath)
        return df
