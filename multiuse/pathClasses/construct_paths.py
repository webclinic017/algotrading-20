"""Construct paths."""
# %% codecell
from pathlib import Path

try:
    from scripts.dev.multiuse.help_class import getDate
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
except ModuleNotFoundError:
    from multiuse.help_class import getDate
    from multiuse.path_helpers import get_most_recent_fpath

# %% codecell


class PathConstructs():
    """Construct paths."""

    @staticmethod
    def hist_sym_fpath(sym, bpath, dt=getDate.query('iex_close')):
        """Symbol and base_path directory."""
        if not isinstance(bpath, Path):
            bpath = Path(bpath)

        fsuf = f"{sym.lower()[0]}/_{sym}.parquet"
        fpath = bpath.joinpath(str(dt.year), fsuf)
        return fpath

    @staticmethod
    def path_or_yr_direct(bdir):
        """Pass a directory in, see if any directories match."""
        r15 = range(15)
        dt = getDate.query('iex_close')
        neg_yr_list = [f"_{str(dt.year - n)}" for n in r15]
        pos_yr_list = [f"_{str(dt.year + n)}" for n in r15]
        yr_dir_list = neg_yr_list + pos_yr_list

        dirs = [f for f in list(bdir.iterdir()) if f.name in yr_dir_list]

        return dirs

    @staticmethod
    def most_recent_fpath(*args, **kwargs):
        """Pass all arguments to get_most_recent_fpath."""
        return get_most_recent_fpath(*args, **kwargs)
