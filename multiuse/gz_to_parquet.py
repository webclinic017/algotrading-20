"""Convert gz files to parquet."""
import os
from time import sleep
from pathlib import Path
import gc
from gzip import BadGzipFile

from tqdm import tqdm
import pandas as pd


try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, help_print_arg, write_to_parquet

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
        gc.set_threshold(100, 5, 5)

        for fpath in tqdm(fpath_list):
            size = os.path.getsize(str(fpath)) / 1000000
             # If size < 250 mb
            if size < 250 and 'apca' not in str(fpath) and 'intraday' not in str(fpath):
                try:
                    self._read_write_file(self, fpath)
                except Exception as e:
                    msg = f"{type(e)} : {str(fpath)} : {str(e)}"
                    help_print_arg(msg)
                    fpath_exc_list.append(fpath)
                    exc_list.append(msg)

        self.exc_list += exc_list
        self.fpath_exc_list += fpath_exc_list

    @classmethod
    def _read_write_file(cls, self, fpath):
        """Read, write, and free up memory."""
        df, fpath_write = None, None

        if '.json' in str(fpath):
            fpath_write = f"{str(fpath)[:-5]}.parquet"
        elif '.gz' in str(fpath):
            fpath_write = f"{str(fpath)[:-3]}.parquet"

        if not Path(fpath_write).exists():
            try:
                df = pd.read_json(fpath)
            except UnicodeDecodeError:
                df = pd.read_parquet(fpath)
            except BadGzipFile:
                df = pd.read_parquet(fpath)

            if isinstance(df, pd.DataFrame):
                write_to_parquet(df, fpath_write)

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
