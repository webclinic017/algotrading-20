"""Path dict helper."""
# %% codecell

class FpathList():
    """Dict of commonly used fpaths: FpathList.fpaths."""

    """
    self.fpath_dict : dict of all fpaths (including most_recent)
    self.fpath : final fpath to return
    """

    def __init__(self, key, most_recent=True):
        self._make_fpath_dict(self)
        self._get_fpath(self, key)

    @classmethod
    def _make_fpath_dict(cls, self):
        """Make dictionary of fpaths."""
        bpath = Path(baseDir().path)
        miss_dates_pre = 'StockEOD/missing_dates'

        fbase_dir = ({
            'hist_miss_less_20': Path(bpath, miss_dates_pre, 'less_than_20')
        })

        fpath_dict = ({
            'hist_null_dates': Path(bpath, miss_dates_pre, 'null_dates/_null_dates.parquet'),
            'hist_miss_less_20': get_most_recent_fpath(fbase_dir['hist_miss_less_20']),
        })

        self.fpath_dict = fpath_dict

    @classmethod
    def _get_fpath(cls, self, key):
        """Get fpath


# %% codecell
