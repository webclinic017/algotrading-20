"""Analyst Paths."""
# %% codecell

from pathlib import Path

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate
    from scripts.dev.multiuse.pathClasses.construct_paths import PathConstructs
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate
    from multiuse.pathClasses.construct_paths import PathConstructs


# %% codecell


class AnalystRecsPaths(PathConstructs):
    """Analyst recommendation paths that can be inherited."""
    fdict = {}
    dt = getDate.query('iex_eod')
    year = str(dt.year)

    def __init__(self, **kwargs):
        self.return_df = kwargs.get('return_df', False)
        self.fdict['earnings'] = self._earnings_fpaths(self, **kwargs)
        self.fdict['prices'] = self._prices_fpaths(self, **kwargs)

    @classmethod
    def _earnings_fpaths(cls, self, base={}, **kwargs):
        """Get full list of analyst earnings_fpaths."""
        bdir = Path(baseDir().path, 'economic_data', 'analyst_earnings')
        base['paths'] = list(bdir.rglob('*.parquet'))
        base['most_recent'] = (self.most_recent_fpath(
                               bdir.joinpath(f"_{self.year}")))
        return base

    @classmethod
    def _prices_fpaths(cls, self, base={}, **kwargs):
        """Get analyst pricing estimates."""
        bdir = Path(baseDir().path, 'company_stats', 'analyst_recs')
        fpath = bdir.joinpath(f"_{self.year}.parquet")

        base['paths'] = self.path_or_yr_direct(bdir)
        base['most_recent'] = fpath


# %% codecell
