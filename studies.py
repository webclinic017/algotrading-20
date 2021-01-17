import pandas as pd
import numpy as np


class regStudies():
    """Implement Standard Studies."""
    df = pd.DataFrame()
    
    def __init__(self, df):
        df = self.rsi(self, df)
        df = self.obv(self, df)
        df = self.ad(self, df)
        self.df = df

    @classmethod
    def rsi(cls, self, df):
        """RSI Implementation."""
        n = 14
        
        def rma(x, n, y0):
            a = (n-1) / n
            ak = a**np.arange(len(x)-1, -1, -1)
            return np.append(y0, np.cumsum(ak * x) / ak / n + y0 * a**np.arange(1, len(x)+1))
        
        df['change'] = df['fClose'].diff()
        df['gain'] = df.change.mask(df.change < 0, 0.0)
        df['loss'] = -df.change.mask(df.change > 0, -0.0)
        df.loc[n:,'avg_gain'] = rma( df.gain[n+1:].values, n, df.loc[:n, 'gain'].mean())
        df.loc[n:,'avg_loss'] = rma( df.loss[n+1:].values, n, df.loc[:n, 'loss'].mean())
        df['rs'] = df.avg_gain / df.avg_loss
        df['rsi_14'] = 100 - (100 / (1 + df.rs))
        
        return df
    
    @classmethod
    def obv(cls, self, df):
        """On Balance Volume."""
        # OBV method #2
        df['OBV_vol'] = df['fVolume'].copy()
        df['OBV_test'] = df['fVolume'].copy()

        df.loc[0, 'OBV_vol'], df.loc[0, 'OBV_test'] = 0, 0
        df['OBV_test'].where(df['change'] > 0, - df['OBV_vol'], inplace=True)
        df['OBV_test'] = df['OBV_test'].cumsum()
        
        return df
    
    @classmethod
    def ad(cls, self, df):
        """Accumulation/Distribution."""
        # Accumulation/Distribution - A/D
        # A/D = Prev A/D + CMFV (current money flow volume)
        #       (Pc - Pl) - (Ph - Pc)                | Pl = Price low   Ph = Price high
        #     = ----------------------   * V         | V = Volume
        #              Ph - Pl                       | Pc = Price close

        df['A/D'] = 0
        df['A/D'] =  ((((df['fClose'] - df['fLow']) -            
                         (df['fHigh'] - df['fClose'])) /
                         (df['fHigh'] - df['fLow']))
                        * df['volume']
                     )
        df['A/D_cum'] = df['A/D'].cumsum()
        
        return df
    
    @staticmethod
    def return_df(self):
        """Return dataframe."""
        return self.df
    