"""Parse incoming messages and store locally."""
# %% codecell
from pathlib import Path


import pandas as pd
from pandas.api.types import infer_dtype

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg, write_to_parquet

# %% codecell


class TelegramParseMsgs():
    """Parse telegram messages."""
    # df can be a dataframe, can also include path to pkl

    def __init__(self, df):
        mx_list = self._get_mixed_list(self, df)
        dtype_dict, ok_vals = self._get_ok_vals_dtype_dict(self, df, mx_list)
        dfs_exp = self._expand_dict_list_rows(self, df, mx_list, dtype_dict)
        df_all = self._make_df_all(self, df, dfs_exp, ok_vals)
        self.df, self.df_all = self._write_all_and_original(self, df, df_all)

    @classmethod
    def _get_mixed_list(cls, self, df):
        """Get list of mixed dtype columns."""
        # Integer column can't be a list or dict. Has to be the object column
        o_df = df.select_dtypes(include=['O'])
        ocols = o_df.columns

        # %% codecell
        mx_list = []
        # Catchall, so can be assumded that there is at least one list, dict
        for col in ocols:
            if 'mixed' in infer_dtype(o_df[col].values):
                mx_list.append(col)

        return mx_list

    @classmethod
    def _get_ok_vals_dtype_dict(cls, self, df, mx_list):
        """Get the non dict/list rows. Record other data types."""
        dtype_dict = {}
        ok_vals = {}
        for col in mx_list:
            for index, row in df[col].iteritems():
                if isinstance(row, dict):
                    dtype_dict[index] = 'dict'
                elif isinstance(row, list):
                    dtype_dict[index] = 'list'
                else:
                    ok_vals[index] = row
                    # dtype_dict[index] = row
        return dtype_dict, ok_vals

    @classmethod
    def _expand_dict_list_rows(cls, self, df, mx_list, dtype_dict):
        """Get list of single row data frames."""
        dfs_exp = []

        for col in mx_list:
            for idx, val in dtype_dict.items():
                row = df.loc[idx][col]
                if val == 'dict':
                    df_n = pd.json_normalize(row).add_prefix(f"{idx}_")
                    dfs_exp.append(df_n)
                if val == 'list':
                    if len(row) == 1:
                        if isinstance(row[0], dict):
                            df_n = pd.json_normalize(row).add_prefix(f"{idx}_")
                            dfs_exp.append(df_n)
                        elif isinstance(row[0], list):
                            print('Not another list')
                        else:
                            # df.loc[idx][col] = row[0]
                            pass

        return dfs_exp

    @classmethod
    def _make_df_all(cls, self, df, dfs_exp, ok_vals):
        """Make df_all from expanded telegram message json."""
        df_conc = pd.concat(dfs_exp, axis=1)
        df_others = pd.DataFrame.from_dict(ok_vals, orient='index').T
        df_all = df_others.join(df_conc)
        df_all['update_id'] = df['update_id'].iloc[0]
        df_all['date'] = pd.to_datetime(df_all['date'], unit='s')

        vc_ui = df['update_id'].value_counts()
        if len(vc_ui.index) != 1:
            help_print_arg("ParseTelegramMsgs: different update_id")

        return df_all

    @classmethod
    def _write_all_and_original(cls, self, df, df_all):
        """Write original df and cleaned df to local file."""
        bpath = Path(baseDir().path, 'social', 'telegram', 'messages')
        fpath = bpath.joinpath('_tradeconfirms.parquet')
        fpath_pkl = fpath.parent.joinpath(f"{fpath.stem}.pickle")

        write_to_parquet(df_all, fpath, combine=True)

        if fpath_pkl.exists():
            df_pk = pd.read_pickle(fpath_pkl)
            df = pd.concat([df, df_pk])

        df.to_pickle(fpath_pkl)

        return df, df_all

# %% codecell
