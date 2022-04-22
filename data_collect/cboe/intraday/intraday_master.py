"""Cboe Intraday Master."""
# %% codecell

from pathlib import Path

try:
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg
    from scripts.dev.cboe.intraday.intraday_clean import CboeIntraCleanP2
except ModuleNotFoundError:
    from multiuse.class_methods import ClsHelp
    from multiuse.help_class import baseDir, help_print_arg
    from cboe.intraday.intraday_clean import CboeIntraCleanP2


# %% codecell


class CboeIntraCleanP1(ClsHelp):
    """Clean cboe intraday."""

    def __init__(self, **kwargs):
        self._cicp1_get_class_vars(self, **kwargs)
        self._cicp1_iterate_paths(self, **kwargs)

    @classmethod
    def _cicp1_get_class_vars(cls, self, **kwargs):
        """Get class variables for cboe intra day."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')

        bpath_cboe = (Path(baseDir().path, 'derivatives', 'cboe_intraday',
                                           '2022'))
        self.paths = ([f for f in list(bpath_cboe.rglob('*.parquet'))
                      if '_intraday' in str(f.stem)])

        if not self.paths:
            help_print_arg(f"""CboeIntraClean no paths with\
                            bpath {str(bpath_cboe)}""")

    @classmethod
    def _cicp1_iterate_paths(cls, self, **kwargs):
        """Iterate through fpaths."""
        for f in self.paths:
            f_cboe_intra = Path(f)
            f_cboe3_clean = (f_cboe_intra.parent.parent
                             .joinpath('cleaned_intra', '2022',
                                       f"{f_cboe_intra.stem}.parquet"))
            if not self.testing:
                try:
                    (CboeIntraCleanP2(f_cboe_intra=f_cboe_intra,
                                      f_cboe3_clean=f_cboe3_clean,
                                      **kwargs))
                except Exception as e:
                    self.elog(self, e)
                    continue
            else:
                (CboeIntraCleanP2(f_cboe_intra=f_cboe_intra,
                                  f_cboe3_clean=f_cboe3_clean,
                                  **kwargs))
