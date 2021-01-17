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
        n = 5  # Number of points to be checked before and after
        
        # Find local peaks
        df['localMin'] = df.iloc[argrelextrema(df.fClose.values, np.less_equal,
                    order=n)[0]]['fClose']
        df['localMax'] = df.iloc[argrelextrema(df.fClose.values, np.greater_equal,
                    order=n)[0]]['fClose']
        
        return df
    
    @staticmethod
    def return_df(self):
        """Return dataframe."""
        return self.df