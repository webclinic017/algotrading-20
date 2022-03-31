"""Reddit helpers."""
# %% codecell

from pathlib import Path
import pprint
from collections import defaultdict
import pandas as pd


try:
    from scripts.dev.multiuse.help_class import baseDir
except ModuleNotFoundError:
    from multiuse.help_class import baseDir

# %% codecell


class RedditHelpers():
    """Base class for reddit helpers, fpaths."""

    def __repr__(self):
        return self._print_path_options()

    @staticmethod
    def _print_path_options():
        """Print fpath options."""
        mdict = ({
            'ref_subs': 'subreddit reference',
            'sub_comments': 'subreddit comments. subreddit kwarg required'
        })

        pp = pprint.PrettyPrinter()
        pp.pprint(mdict)
        return ''

    @staticmethod
    def _get_path(method, **kwargs):
        """Get local filepath."""
        bdir = Path(baseDir().path, 'social', 'reddit')
        subreddit = kwargs.get('subreddit', 'wallstreetbets')
        return_df = kwargs.get('return_df', False)

        mdict_base = ({
            'ref_subs': bdir.joinpath('ref', '_subreddits.parquet'),
            'sub_comments': bdir.joinpath('subs', subreddit, '_comments.parquet'),
            'sub_authors': bdir.joinpath('subs', subreddit, '_authors.parquet'),
            '': Path()
        })

        def default_path():
            return Path()

        mdict = defaultdict(default_path, mdict_base).copy()

        fpath = mdict[method]
        if fpath.exists() and return_df:
            result = pd.read_parquet(fpath)
            return result
        else:
            result = fpath
            return result


# %% codecell
