"""Clean TD Ameritrade Options."""
# %% codecell
from pathlib import Path

import pandas as pd
import numpy as np

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet
    from scripts.dev.multiuse.df_helpers import DfHelpers
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet
    from multiuse.df_helpers import DfHelpers


# %% codecell


class TD_Clean_Write_Options():
    """Clean and write TD Options."""

    """
    param: self.verbose : to print out errors or not
    param: self.fpath : fpath used for main options writing
    param: self.fpath_sum : fpath used for summary dataframe
    param: self.df_sum : summary dataframe
    param: self.df : dataframe of all row data
    """

    def __init__(self, gjson, sym, dt=None, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self._add_params(self)
        self.fpath = self._construct_fpaths(self, dt, sym)
        self._read_clean_summary_data(self, gjson, sym)
        self.df = self._read_clean_options_tree(self, gjson, sym)

    @classmethod
    def _add_params(cls, self):
        """Add params if necessary."""
        pass

    @classmethod
    def _construct_fpaths(cls, self, dt, sym):
        """Construct fpaths to use for reg/summary."""
        bpath = Path(baseDir().path, 'derivatives', 'tdma')

        if not dt:  # If no date passed, assume most recent
            dt = getDate.query('iex_eod')

        sym1 = sym.lower()[0]
        sym2 = f"_{sym}.parquet"
        sympath = Path(str(dt.year), sym1, sym2)
        fpath = bpath.joinpath('series', sympath)
        self.fpath_sum = bpath.joinpath('summary', sympath)

        return fpath

    @classmethod
    def _read_clean_summary_data(cls, self, gjson, sym):
        """Process and write summary data to local file."""
        df_sum = pd.DataFrame(gjson['underlying'], index=range(1))
        df_sum['underlying'] = sym
        # Convert ms timestamps to datetiem
        cols_to_dt = ['quoteTime', 'tradeTime']
        for col in cols_to_dt:
            df_sum[col] = pd.to_datetime(df_sum[col], unit='ms', errors='coerce')

        self.df_sum = df_sum
        # Write data to parquet
        write_to_parquet(df_sum, self.fpath_sum, combine=True)

    @classmethod
    def _read_clean_options_tree(cls, self, gjson, sym):
        """For loop for nested json value. Convert to df."""
        opt_names = ['putExpDateMap', 'callExpDateMap']

        row_list = []
        # Extract row data from nested dictionary
        for k1 in opt_names:
            for k2 in gjson[k1].keys():
                for k3 in gjson[k1][k2].keys():
                    try:
                        row_list.append(gjson[k1][k2][k3][0])
                    except IndexError:
                        print(gjson[k1][k2][k3])
        # Convert list of rows into dataframe
        td_df = pd.DataFrame(row_list).reset_index(drop=True)
        td_df['underlying'] = sym
        row_to_dt = (['tradeTimeInLong', 'quoteTimeInLong',
                      'expirationDate', 'lastTradingDay'])
        for row in row_to_dt:
            td_df[row] = pd.to_datetime(td_df[row], unit='ms', errors='coerce')
        # Convert columns
        cols_to_convert = (['volatility', 'delta', 'gamma',
                            'theta', 'vega', 'rho', 'theoreticalOptionValue'])
        for col in cols_to_convert:
            td_df[col] = td_df[col].astype(np.float64)

        # Handle NaNs
        cols = td_df.columns
        for col in cols:
            # If the sum total of NaNs is greater than 0
            if DfHelpers.check_nan(td_df[col]).sum() > 0:
                td_df[col] = td_df[col].astype(np.float64)
                td_df.loc[:, col] = (np.where(DfHelpers.check_nan(td_df[col]),
                                     np.NaN, td_df[col]))
        # Add a column for access date for each symbol, that way we can
        # drop based on it
        td_df['dt_symbol'] = (td_df['quoteTimeInLong'].dt.strftime('%Y%m%d')
                              + '_' + td_df['symbol'])

        # Columns to convert to categorical
        cols_to_cat = (['putCall', 'symbol', 'description', 'exchangeName',
                        'bidAskSize', 'optionDeliverablesList',
                        'expirationType', 'settlementType', 'deliverableNote',
                        'underlying', 'dt_symbol', 'date'])
        td_df[cols_to_cat] = td_df[cols_to_cat].astype('category')
        # Write dataframe to local file
        kwargs = {'cols_to_drop': ['dt_symbol'], 'cols_to_cat': cols_to_cat}
        write_to_parquet(td_df, self.fpath, combine=True, **kwargs)

        return td_df

# %% codecell
