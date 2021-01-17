import pandas as pd
import numpy as np


class movingAverages():
    """Implement Moving Averages."""

    # Adjusted closing price
    close_to_use = 'fClose'
    ma_list = [10, 20, 50, 200]
    df = pd.DataFrame()

    def __init__(self, df):
        df = self.basic_col_calc(df)
        df = self.sma(self, df, self.close_to_use)
        df = self.cma(self, df, self.close_to_use)

        (ma_crossovers, ma_differences,
         cma_crossovers, cma_differences) = self.build_ma_lists(df, self.ma_list)

        f_price_levels, f_price_dict = self.build_ma_lists_diff(self)
        df = self.add_ma_columns(self, df,
                                 f_price_dict,
                                 f_price_levels)
        df = self.ma_cross(df, ma_crossovers, ma_differences)
        df = self.ma_cross(df, cma_crossovers, cma_differences)
        # Store df in local variable
        self.df = df

    @classmethod
    def basic_col_calc(cls, df):
        """Basic column calculations."""
        df['fRange'] = df['fHigh'] - df['fLow']
        return df

    @classmethod
    def sma(cls, self, df, close_to_use):
        """Simple Moving Averages."""
        df['sma_10'] = df[close_to_use].rolling(window=10).mean()
        df['sma_20'] = df[close_to_use].rolling(window=20).mean()
        df['sma_50'] = df[close_to_use].rolling(window=50).mean()
        df['sma_200'] = df[close_to_use].rolling(window=200).mean()

        df = self.sma_diff(self, df)
        return df

    @classmethod
    def sma_diff(cls, self, df):
        """SMA differences."""
        df['sma_diff_10_20'] = df['sma_10'] - df['sma_20']
        df['sma_diff_10_50'] = df['sma_10'] - df['sma_50']
        df['sma_diff_10_200'] = df['sma_10'] - df['sma_200']
        df['sma_diff_20_50'] = df['sma_20'] - df['sma_50']
        df['sma_diff_20_200'] = df['sma_20'] - df['sma_200']
        df['sma_diff_50_200'] = df['sma_50'] - df['sma_200']

        return df

    @classmethod
    def cma(cls, self, df, close_to_use):
        """Exponential Moving Average."""
        df['cma_10'] = df[close_to_use].ewm(
            span=10, min_periods=10, adjust=False, ignore_na=False
            ).mean()
        df['cma_20'] = df[close_to_use].ewm(
            span=20, min_periods=20, adjust=False, ignore_na=False
            ).mean()
        df['cma_50'] = df[close_to_use].ewm(
            span=50, min_periods=50, adjust=False, ignore_na=False
            ).mean()
        df['cma_200'] = df[close_to_use].ewm(
            span=200, min_periods=200, adjust=False, ignore_na=False
            ).mean()

        df = self.cma_diff(self, df)
        return df

    @classmethod
    def cma_diff(cls, self, df):
        """Exponential Moving Average Differences."""
        df['cma_diff_10_20'] = df['cma_10'] - df['cma_20']
        df['cma_diff_10_50'] = df['cma_10'] - df['cma_50']
        df['cma_diff_10_200'] = df['cma_10'] - df['cma_200']
        df['cma_diff_20_50'] = df['cma_20'] - df['cma_50']
        df['cma_diff_20_200'] = df['cma_20'] - df['cma_200']
        df['cma_diff_50_200'] = df['cma_50'] - df['cma_200']

        return df

    @classmethod
    def build_ma_lists(cls, df, ma_list):
        """Build Column Names for MA."""
        # Example - 'sma_cross_10_20'
        n = 0
        ma_crossovers = []
        ma_differences = []
        cma_crossovers = []
        cma_differences = []

        for xe, x in enumerate(ma_list):
            n += 1
            for ye,y in enumerate(ma_list):
                ye += n
                try:
                    # print(ma_list[xe], ma_list[ye])
                    ma_crossovers.append(f"sma_cross_{ma_list[xe]}_{ma_list[ye]}")
                    ma_differences.append(f"sma_diff_{ma_list[xe]}_{ma_list[ye]}")
                    cma_crossovers.append(f"cma_cross_{ma_list[xe]}_{ma_list[ye]}")
                    cma_differences.append(f"cma_diff_{ma_list[xe]}_{ma_list[ye]}")
                except IndexError:
                    pass

        # Add empty crossover columns
        for x, y in zip(ma_crossovers, cma_crossovers):
            df[x] = 0
            df[y] = 0
        """
        print(ma_crossovers)
        print()
        print(ma_differences)
        print()
        print(cma_crossovers)
        print()
        print(cma_differences)
        """
        return ma_crossovers, ma_differences, cma_crossovers, cma_differences

    @classmethod
    def build_ma_lists_diff(cls, self):
        """Build Column Names for MA Differences."""
        # Price differences between moving
        f_price_levels = ['fClose', 'fHigh', 'fLow']
        f_price_dict = {}
        for x in f_price_levels:
            f_price_dict[f"{x}_sma_crossovers"] = []
            f_price_dict[f"{x}_cma_crossovers"] = []

        for xe,x in enumerate(f_price_dict):
            for y in self.ma_list:
                if xe <= 1:
                    if xe != 1:
                        f_price_dict[x].append(f"fClose_sma_diff_{y}")
                    else:
                        f_price_dict[x].append(f"fClose_cma_diff_{y}")
                elif xe <= 3:
                    if xe != 3:
                        f_price_dict[x].append(f"fHigh_sma_diff_{y}")
                    else:
                        f_price_dict[x].append(f"fHigh_cma_diff_{y}")
                elif xe <= 5:
                    if xe != 5:
                        f_price_dict[x].append(f"fLow_sma_diff_{y}")
                    else:
                        f_price_dict[x].append(f"fLow_cma_diff_{y}")
                else:
                    print('There was an error determining the iteration level')

        return f_price_levels, f_price_dict

    @classmethod
    def add_ma_columns(cls, self, df, f_price_dict, f_price_levels):
        """Create MA columns in df."""
        for x in f_price_dict:
            for y, z in zip(f_price_dict[x], self.ma_list):
                 # print(y)
                if 'sma' in str(y):
                    for pl in f_price_levels:
                        if str(pl) in str(y):
                            df[y] = df[pl] - df[f"sma_{z}"]
                elif 'cma' in str(y):
                    for pl in f_price_levels:
                        if str(pl) in str(y):
                            df[y] = df[pl] - df[f"cma_{z}"]
                else:
                    print('Neither sma nor cma could be found in the f_price_dict... passing')
                    print(y)
                    print()
                    pass

        return df

    @classmethod
    def ma_cross(cls, df, crossovers, differences):
        """Binary identification of moving averages crossing each other."""
        for x, y in zip(crossovers, differences):
            df[x] = np.where(
                            df[y].where(
                                np.sign(df[y]) !=
                                np.sign(df[y]).shift(1)
                            ).fillna(0),
                            1, 0)
        return df

    @staticmethod
    def return_df(self):
        """Return the dataframe."""
        return self.df
