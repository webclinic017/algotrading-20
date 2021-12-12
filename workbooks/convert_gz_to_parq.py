"""Convert all local files to parquet files."""
# %% codecell
from pathlib import Path
from tqdm import tqdm

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, write_to_parquet, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, write_to_parquet, help_print_arg
# %% codecell


class JsonToParquet():
    """Convert all local files from json/gz to parquet."""

    """
    self.all_json : all files with .json ending
    self.all_gz : all files with .gz ending
    """

    def __init__(self):
        self._get_dir_lists(self)
        self._file_change_loop(self, self.all_json)
        self._file_change_loop(self, self.all_gz)

    @classmethod
    def _get_dir_lists(cls, self):
        """Get json and gz lists."""
        bpath = Path(baseDir().path)
        self.all_json = list(bpath.glob('**/*.json'))
        self.all_gz = list(bpath.glob('**/*.gz'))

        self.exc_list = []
        self.fpath_exc_list = []

    @classmethod
    def _file_change_loop(cls, self, fpath_list):
        """Start the for loop for file type change."""
        exc_list = []
        fpath_exc_list = []

        for fpath in tqdm(fpath_list):
            try:
                df = pd.read_json(fpath)
                write_to_parquet(df, fpath)
            except Exception as e:
                msg = f"{type(e)} : {str(fpath)} : {str(e)}"
                help_print_arg(msg)
                fpath_exc_list.append(fpath)
                exc_list.append(msg)

        self.exc_list += exc_list
        self.fpath_exc_list += fpath_exc_list

    @classmethod
    def _write_error_lists(cls, self):
        """Write error lists to local files."""
        fpath_str_list = [str(f) for f in self.fpath_exc_list]
        fpath_df = pd.Series(fpath_str_list, name='fpath').to_frame()
        try:
            fpath_df['error'] = self.exc_list
        except ValueError:
            pass

        path_to_write = Path(baseDir().path, 'errors', 'gz_to_parquet_fpaths.parquet')
        fpath_df.to_parquet(path_to_write)


# %% codecell


# %% codecell

# %% codecell


# %% codecell

# %% codecell
