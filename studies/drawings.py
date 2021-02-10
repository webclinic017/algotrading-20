import pandas as pd
import numpy as np
from scipy.signal import argrelextrema

class makeDrawings():
    """Drawings/Patterns Implementation."""
    df = pd.DataFrame()
    
    def __init__(self, df):
        df = self.local_max_min(self, df)
        self.df = df
        
    @classmethod
    def local_max_min(cls, self, df):
        """Make Local Maximum and Minimums."""
        # Number of points to be checked before and after
        xlist = [5, 10, 20, 50, 200]
        
        # Find local peaks
        for xn,x in enumerate(xlist):
            df[f"localMin_{x}"] = df.iloc[argrelextrema(
                    df.fClose.values, np.less_equal, order=x
                    )[0]]['fClose']
            df[f"localMax_{x}"] = df.iloc[argrelextrema(
                    df.fClose.values, np.greater_equal,
                    order=x)[0]]['fClose']
        
        return df
    
    @staticmethod
    def return_df(self):
        """Return dataframe."""
        return self.df