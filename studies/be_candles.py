import pandas as pd
import numpy as np

def be_prints(df):
    """Bearish column definitions and date printouts."""
    # Create function attribute that can be called outside scope
    be_prints.bear_candles = {
    'cd_best': {'column': 'cd_best', 'name': 'Bearish Spinning Top', 'candle': 1},
    'cd_bedd': {'column': 'cd_bedd', 'name': 'Bearish Dragonfly Doji', 'candle': 1},
    'cd_behm': {'column': 'cd_behm', 'name': 'Bearish Hanging Man', 'candle': 1},
    'cd_besstar': {'column': 'cd_besstar', 'name': 'Bearish Shooting Star', 'candle': 1},
    'cd_begstone': {'column': 'cd_begstone', 'name': 'Bearish Grave Stone', 'candle': 1},    
    'cd_bekick': {'column': 'cd_bekick', 'name': 'Bearish Kicker', 'candle': 2},
    'cd_begulf': {'column': 'cd_begulf', 'name': 'Bearish Engulfing', 'candle': 2},
    'cd_beharam':  {'column': 'cd_beharam' , 'name': 'Bearish Harami', 'candle': 2},
    'cd_bedcc':  {'column': 'cd_bedcc',  'name': 'Bearish Dark Cloud Cover', 'candle': 2},
    'cd_bettop': {'column': 'cd_bettop', 'name': 'Bearish Tweezer Top', 'candle': 2},
    'cd_beababy': {'column': 'cd_beababy', 'name': 'Bearish Abandoned Baby', 'candle': 3},
    'cd_betbc':  {'column': 'cd_betbc', 'name': 'Bearish 3 Black Crows', 'candle': 3},
    'cd_beeds':  {'column': 'cd_beeds', 'name': 'Bearish Evening Doji Star', 'candle': 3},
    'cd_bees':  {'column': 'cd_bees', 'name': 'Bearish Evening Star', 'candle': 3}
        
    }
    
    getBeCandles = beCandles()
    
    for be in be_prints.bear_candles:
        # print(str(be)[3:])
        eval(f"getBeCandles.{str(be)[3:]}")(df)
        
    
    for be in be_prints.bear_candles:
        print(be_prints.bear_candles[be]['name'])
        print('##################################################')
        print(df[df[be] == -1]['date'])
        print()


class beCandles():
    """Candlestick analysis 1-2-3 day."""
    
    @staticmethod
    def best(df):
        """Bearish spinning top."""
        # Bearish spinning top - BEST

        best_cutoff = .20 # 20% of the days range
        best_tail_cutoff = .40 # 

        df['cd_best'] = np.where(
                                (   # Close between open +/- (range * .20)
                                    df['fClose'].between(
                                        (df['fOpen'] - 
                                        (df['fRange'] * .20)), 
                                        (df['fOpen'] + 
                                        (df['fRange'] * .20))
                                    )
                                &   # Close < open
                                    (df['fClose'] < df['fOpen'])
                                &   # High > open + (range * .40)
                                    df['fHigh'].where(
                                        df['fHigh'] > 
                                       (df['fOpen'] + 
                                       (df['fRange'] * .40))
                                    )
                                &   df['fLow'].where(
                                        df['fLow'] < 
                                       (df['fOpen'] - 
                                       (df['fRange'] * .40))
                                    )
                                ), -1, 0)

        # print(df['cd_best'].value_counts())
        return df
    
    @staticmethod
    def bedd(df):
        """Bearish Dragonfly Doji."""
        # Open and close the same, big top shadow, no bottom shadow

        df['cd_bedd'] = np.where(
                                (   # Close between open +/- (range * .05)
                                    df['fClose'].between(
                                        (df['fOpen'] - (df['fRange'] * .05)), 
                                        (df['fOpen'] + (df['fRange'] * .05))
                                    )
                                &   # High > open + (range * .3)
                                    df['fHigh'].where(
                                         df['fHigh'] > 
                                        (df['fOpen'] + 
                                        (df['fRange'] * .3))
                                    )
                                &   # Low > open - (range * .15)
                                    df['fLow'].where(
                                        df['fLow'] >
                                       (df['fOpen'] - (df['fRange'] * .15))
                                    )
                                ), -1, 0)

        # print(df['cd_bedd'].value_counts())
        return df
    
    
    @staticmethod
    def behm(df):
        """Bearish Hanging Man."""
        # Close below open, no top shadow, big bottom shadow
        df['cd_behm'] = np.where(
                                (   # Close between open - (range * .20), open
                                    df['fClose'].between(
                                        (df['fOpen'] - 
                                        (df['fRange'] * .20)), 
                                        (df['fOpen'])
                                    )
                                &   # High < open + (range * .05)
                                    df['fHigh'].where(
                                        df['fHigh'] <
                                       (df['fOpen'] + 
                                       (df['fRange'] * .05))
                                    )
                                &   # Low < open - (range * .75)
                                    df['fLow'].where(
                                        df['fLow'] <
                                       (df['fOpen'] - 
                                       (df['fRange'] * .75))
                                    )
                                ), -1, 0)

        # print(df['cd_behm'].value_counts())
        return df
    
    
    @staticmethod
    def besstar(df):
        """Bearish Shooting Star."""
        # Close below open, big top shadow, no bottom shadow
        df['cd_besstar'] = np.where(
                                (   # Close between open - (range * .25), open
                                    df['fClose'].between(
                                        (df['fOpen'] - 
                                        (df['fRange'] * .25)), 
                                        (df['fOpen'])
                                    )
                                &   # High > open + (range * .50)
                                    df['fHigh'].where(
                                         df['fHigh'] >
                                        (df['fOpen'] + 
                                        (df['fRange'] * .50))
                                    )
                                &   # Low > open - (range * .05)
                                    df['fLow'].where(
                                         df['fLow'] >
                                        (df['fOpen'] - 
                                        (df['fRange'] * .05))
                                    )
                                ), -1, 0)

        # print(df['cd_besstar'].value_counts())
        return df
    
    
    @staticmethod
    def begstone(df):
        """Bearish Gravestone Doji."""
        # Close just below open, big top shadow, no bottom shadow
        df['cd_begstone'] = np.where(
                                (   # Close between open - (range * .05), open
                                    df['fClose'].between(
                                        (df['fOpen'] - 
                                        (df['fRange'] * .25)), 
                                         df['fOpen']
                                    )
                                &   # High > open + (range * .50)
                                    df['fHigh'].where(
                                        df['fHigh'] >
                                       (df['fOpen'] + 
                                       (df['fRange'] * .50))
                                    )
                                &   # Low > open - (range * .05)
                                    df['fLow'].where(
                                        df['fLow'] >
                                       (df['fOpen'] - 
                                       (df['fRange'] * .05))
                                    )
                            ),
                            np.where(  # Candlestick -1
                                (  # At least .25% upwards
                                   (df['changePercent'].shift(1, axis=0) > .0025)
                                &  # -1 close > -2 close
                                   (df['fClose'].shift(1, axis=0) >
                                    df['fClose'].shift(2, axis=0)
                                   )
                            ),    
                            np.where( # Candlestick 2
                                (  # At least -.25 downwards
                                   (df['changePercent'].shift(-1, axis=0) < -.0025)
                                &  # 1 open > 2 open
                                   (df['fOpen'].shift(-1, axis=0) >
                                    df['fOpen'].shift(-2, axis=0)
                                   )
                                )
                            , -1, 0), 0), 0)
                            

        # print(df['cd_begstone'].value_counts())
        return df
    
    
    @staticmethod
    def bekick(df):
        """Bearish Kicker."""
        # 1st candle big incline, small shadows. 
        # 2nd candle big decline, small shadows

        df['cd_bekick'] = np.where(  # Candlestick #1
                    (       # At least .5% upwards movement
                            (df['changePercent'] > .005)
                        &   # High < open + (range * .2)
                            (df['fHigh'] < 
                            (df['fOpen'] + 
                            (df['fRange'] * .2))
                            )
                        &   # Low > close - (range * .2)
                            (df['fLow'] > 
                            (df['fClose'] - 
                            (df['fRange'] * .2))
                            )
                    ),

                    np.where(  # Candlestick #2
                    (      # Needs to move at least .5% negative
                           (df['changePercent'].shift(-1, axis=0) < -.005)
                        &  # 2nd high is less than 1st low
                           (df['fHigh'].shift(-1, axis=0) < 
                           (df['fLow'] + (df['fRange'] * .2)))
                        &  # Top tail less than 20% of the range
                          ((df['fHigh'].shift(-1, axis=0) -
                            df['fOpen'].shift(-1, axis=0)) <
                           (df['fRange'].shift(-1, axis=0) * .2)
                           )
                        &  # Bottom tail less than 20% of the range
                          ((df['fClose'].shift(-1, axis=0) - 
                            df['fLow'].shift(-1, axis=0)) <=
                           (df['fRange'].shift(-1, axis=0) * .2)) 
                    )
                  , -1, 0), 0)

        # print(df['cd_bekick'].value_counts())
        return df
    
    
    @staticmethod
    def begulf(df):
        """Bearish Engulfing."""
        # 1st candle incline with medium downward tail. 
        # 2nd candle decline with medium downward tail. Open > 1st close, Close < 1st open
        
        df['cd_begulf'] = np.where(  # Candlestick #1
                (   # First > .5% increase
                        (df['changePercent'] > .005)
                    &   # If the candlestick closed within the ...
                        (df['fClose'].between(
                            (df['fLow'] + (df['fRange'] * .30)),
                            (df['fLow'] + (df['fRange'] * .80))
                        ))
                    &   # If the bottom tail is less than 1/3 of the range
                        ((df['fClose'] - 
                          df['fLow']) <=
                         (df['fRange'] * .33)
                        )
                ),

                np.where(
                    (   # Second < -.5% decrease
                        (df['changePercent'].shift(-1, axis=0) < -.005)
                    &   # 2nd open > 1st close
                       ((df['fOpen'].shift(-1, axis=0)) >
                         df['fClose'])
                    &   # 2nd close < than the 1st open
                        (df['fClose'].shift(-1, axis=0) < df['fOpen'] )
                    &   # 2nd tail is less than 1/3 of the range
                       ((df['fClose'].shift(-1, axis=0) -
                         df['fLow'].shift(-1, axis=0)) <=
                       ((df['fRange'].shift(-1, axis=0) *.33)))
                )
           , -1, 0), 0)

        # print(df['cd_begulf'].value_counts())
        return df
        
        
    @staticmethod
    def beharam(df):
        """Bearish Harami."""
        # Bearish Harami - BEHARAM - 
        # 1st candle huge incline with medium downward tail. 
        # 2nd candle decline with equal tails. Open < 1st close, Close < 1st open

        df['cd_beharam'] = np.where(  # Candlestick #1
                    (   # First > .05% increase
                        (df['changePercent'] > .005)
                    &   # If the candlestick closed within the ...
                        (df['fClose'].between(
                            (df['fHigh'] - (df['fRange'] * .45)),
                            (df['fHigh'] - (df['fRange'] * .05))
                        ))
                    &   # If the bottom tail is less than 1/3 of the range
                        ((df['fOpen'] - df['fLow']) <=
                         (df['fRange'] * .33))
                ),

                np.where(  # Candlestick #2
                    (   # Second < .25% decrease
                        (df['changePercent'].shift(-1, axis=0) < -.0025)
                    &   # 2nd open < the 1st close. 2nd open > 1st open.
                        ((df['fOpen'].shift(-1, axis=0).between(
                            (df['fOpen'] + (df['fRange'] * .0025)), 
                            (df['fClose'] - (df['fRange'] * .0025))
                        )))
                    &   # 2nd close > 1st open. 2nd close < 1st close.
                        (df['fClose'].shift(-1, axis=0).between(
                            (df['fOpen'] + (df['fRange'] * .0025)),
                            (df['fClose'] - (df['fRange'] * .0025))
                        ))
                    &   # 2nd top tail < 25% of the range
                       ((df['fHigh'].shift(-1, axis=0) -
                         df['fOpen'].shift(-1, axis=0)) <= 
                         (df['fRange'].shift(-1, axis=0) * .25)
                        )
                    &   # 2nd bottom tail < 25% of the range
                        ((df['fClose'].shift(-1, axis=0) -
                          df['fLow'].shift(-1, axis=0)) <= 
                         (df['fRange'].shift(-1, axis=0) * .25)
                        )
                )
               , -1, 0), 0)

        # print(df['cd_beharam'].value_counts())
        return df
    
    
    @staticmethod
    def bedcc(df):
        """Bearish Dark Cloud Cover."""
        # 1st candle incle with m downward tail, m upward tail
        # 2nd candle decline with m tails. Open < 1st close, Close > 1st open, < 1st close

        df['cd_bedcc'] = np.where(  # Candlestick #1
                    (   # First > .25% increase
                        (df['changePercent'] > .005)
                    &   # If the candlestick closed within the ...
                        (df['fClose'].between(
                            (df['fHigh'] - (df['fRange'] * .20)),
                            (df['fHigh'] - (df['fRange'] * .05))
                        ))
                    &   # If the bottom tail is less than 1/3 of the range
                       ((df['fOpen'] - df['fLow']) <=
                        (df['fRange'] * .33))
                ),
                np.where(  # Candlestick #2
                    (   # Second < -.25% decrease
                         (df['changePercent'].shift(-1, axis=0) < -.005)
                    &   # 2nd open > 1st close
                        ( df['fOpen'].shift(-1, axis=0) > 
                         (df['fClose'] + (df['fRange'] * .05)))
                    &   # 2nd close > than the 1st open, < first close
                        (df['fClose'].shift(-1, axis=0).between(
                            (df['fOpen'] + (df['fRange'] * .15)),
                            (df['fClose'] - (df['fRange'] * .15))
                        ))
                )
               , -1, 0), 0)

        # print(df['cd_bedcc'].value_counts())
        return df
    
    
    @staticmethod
    def bettop(df):
        """Bearish Tweezer Top."""
        # 1st candle incline with s tails. Tot tail ~ Tot tail 2nd candle
        # 2nd candle decline with top tail == 1st open. Open < 1st close. Close > 1st open

        df['cd_bettop'] = np.where(  # Candlestick #1
                    (   # First > .5% decrease
                        (df['changePercent'] > .005)
                    &   # If the candlestick closed within the ...
                        (df['fClose'].between(
                            (df['fHigh'] - (df['fRange'] * .40)),
                            (df['fHigh'] - (df['fRange'] * .05))
                        ))
                    &   # If the bottom tail is less than 1/5 of the range
                        ((df['fHigh'] - df['fClose']) <=
                         (df['fRange'] * .20))
                ),
                np.where(  # Candlestick #2
                    (   # Second < -.25% decrease
                        (df['changePercent'].shift(-1, axis=0) < -.0025)
                    &   # 2nd open < the 1st close
                        (df['fOpen'].shift(-1, axis=0).between(
                           (df['fClose'] - (df['fRange'] * .2)),
                           (df['fClose'] + (df['fRange'] * .05))
                        ))
                    &   # 2nd close > than the 1st open
                        ((df['fClose'].shift(-1, axis=0)) > 
                         (df['fOpen'] + (df['fRange'] * .05)))
                    &   # 2nd top tail is less than 1/3 of the range
                        ((df['fHigh'].shift(-1, axis=0) -
                          df['fOpen'].shift(-1, axis=0)) <=
                         (df['fRange'].shift(-1, axis=0) * .33)
                        )
                )
               , -1, 0), 0)

        # print(df['cd_bettop'].value_counts())
        return df
    
    
    @staticmethod
    def beababy(df):
        """Bearish Abandoned Baby."""
        # Bearish Abandoned Baby - BEABABY - 
        # 1st candle - Big green candle, m tails
        # 2nd candle - Small red candle. Ms tails. Open/Close above 1st and 3rd
        # 3rd candle - Big red candle, m tails. Open < 1st close.
    
        df['cd_beababy'] = np.where(  # Candlestick #1
                    (   # First > .33% increase
                        (df['changePercent'] > .0033)
                    &   # Top tail > .20 of range
                       ((df['fHigh'] - df['fClose']) >
                        (df['fRange'] * .2))
                    &   # Bot tail > .20 of range
                       ((df['fOpen'] - df['fLow']) > 
                        (df['fRange'] * .2))
                ),
                np.where(  # Candlestick #2
                    (   # Second > -.01% decrease, < -.4% decrease
                        (df['changePercent'].shift(-1, axis=0).between(
                            -.004, -.0001
                        ))
                    &   # 2nd open > 1st high
                        (df['fOpen'].shift(-1, axis=0) > df['fHigh'])
                    &   # 2nd open > 3rd high
                        (df['fOpen'].shift(-1, axis=0) >
                         df['fHigh'].shift(-2, axis=0))
                    &   # 2nd close > 1st high
                        (df['fClose'].shift(-1, axis=0) > df['fHigh'])
                    &   # 2nd close > 3rd high
                        (df['fClose'].shift(-1, axis=0) >
                         df['fHigh'].shift(-2, axis=0))
                ),
               np.where(  # Candlestick #3
                    (   # Third < -.5% decrease
                        (df['changePercent'].shift(-2, axis=0) < -.005)
                    &   # 3rd open < 1st close
                        (df['fOpen'].shift(-2, axis=0) < 
                         df['fClose'])

                    )
            , -1, 0), 0), 0)

        # print(df['cd_beababy'].value_counts())
        return df
    
    
    @staticmethod
    def betbc(df):
        """Bearish 3 Black Crows."""
        # Bearish 3 Black Crows - BETBC - 
        # 1st candle - Medium red candle. Close < 2nd close. S tail
        # 2nd candle - Medium red candle. Close < 3rd close. S tail
        # 3rd candle - Medium red candle. S tail
    
        betbc_range = .4
        # From .3 to .4, results went from 1 to 5
    
        df['cd_betbc'] = np.where(  # Candlestick #1
                    (   # First < -.25% decrease
                        (df['changePercent'] < -.0025)
                    &   # 1st close > 2nd close
                        (df['fClose'] > df['fClose'].shift(-1, axis=0))
                    &   # Bottom tail < .3 range
                        ((df['fClose'] - df['fLow']) <
                         (df['fRange']) * betbc_range)
                    &   # Top tail < .3 range
                        ((df['fHigh'] - df['fOpen']) <
                         (df['fRange']) * betbc_range)
                ),
                np.where(  # Candlestick #2
                    (   # Second < -.25% decrease
                        (df['changePercent'].shift(-1, axis=0) < -.0025)
                    &   # 2nd close > 3rd close
                        (df['fClose'].shift(-1, axis=0) >
                         df['fClose'].shift(-2, axis=0))
                    &   # Bottom tail < .3 range
                        ((df['fClose'].shift(-1, axis=0) - 
                          df['fLow'].shift(-1, axis=0)) <
                         (df['fRange'].shift(-1, axis=0)) * betbc_range)
                    &   # Top tail < .3 range
                        ((df['fHigh'].shift(-1, axis=0) - 
                          df['fOpen'].shift(-1, axis=0)) <
                         (df['fRange'].shift(-1, axis=0)) * betbc_range)
                ),
               np.where(  # Candlestick #3
                    (   # Third < -.25% decrease
                        (df['changePercent'].shift(-2, axis=0) < -.0025)
                    &   # Open < 2nd open
                        (df['fOpen'].shift(-2, axis=0) <
                         df['fOpen'].shift(-1, axis=0))
                    &   # Open > 2nd close
                        (df['fOpen'].shift(-2, axis=0) >
                         df['fClose'].shift(-1, axis=0))
                    &   # Bottom tail < .3 range
                        ((df['fClose'].shift(-2, axis=0) - 
                          df['fLow'].shift(-2, axis=0)) <
                         (df['fRange'].shift(-2, axis=0)) * betbc_range)
                    &   # Top tail < .3 range
                        ((df['fHigh'].shift(-2, axis=0) - 
                          df['fOpen'].shift(-2, axis=0)) <
                         (df['fRange'].shift(-2, axis=0)) * betbc_range)
                    )
            , -1, 0), 0), 0)

        # print(df['cd_betbc'].value_counts())
        return df
    
    
    @staticmethod
    def beeds(df):
        """Bearish Evening Doji Star."""
        # Bearish Evening Doji Star - BEEDS - 
        # 1st candle - Big green candle. SS tails
        # 2nd candle - Doji with m tails. Open/close ~ 1st close and 3rd open
        # 3rd candle - Medium red candle. S tail. 3rd close > 1st open. 3rd high < 2nd high.
    
        df['cd_beeds'] = np.where(  # Candlestick #1
                    (   # First > .5% increase
                        (df['changePercent'] > .005)
                    &   # 1st close < 2nd high
                        (df['fClose'] < df['fHigh'].shift(-1, axis=0))
                ),
                np.where(  # Candlestick #2
                    (   # Second < -.1% decrease, > -.01% decrease
                        (df['changePercent'].shift(-1, axis=0).between(
                            -.0015, -.0001
                        ))
                    &   # Bottom tail > .05 range
                        ((df['fClose'].shift(-1, axis=0) - 
                          df['fLow'].shift(-1, axis=0)) >
                         (df['fRange'].shift(-1, axis=0)) * .05)
                    &   # Top tail > .05 range
                        ((df['fHigh'].shift(-1, axis=0) - 
                          df['fOpen'].shift(-1, axis=0)) >
                         (df['fRange'].shift(-1, axis=0)) * .05)
                ),
               np.where(  # Candlestick #3
                    (   # Third < -.5% decrease
                        (df['changePercent'].shift(-2, axis=0) < -.005)
                    &   # Third open < 2nd high
                        (df['fOpen'].shift(-2, axis=0) <
                         df['fHigh'].shift(-1, axis=0))
                    &   # 3rd close > 1st open
                        (df['fClose'].shift(-2, axis=0) >
                         df['fOpen'])
                    )
            , -1, 0), 0), 0)

        # print(df['cd_beeds'].value_counts())
        return df
    
    
    @staticmethod
    def bees(df):
        """Bearish Evening Star."""
        # Bearish Evening Star - BEES - 
        # 1st candle - Big green candle. ML tails. 1st high < 2nd low
        # 2nd candle - MS red candle. Gap up. Bot tail > top tail.
        # 3rd candle - Big red candle. MS tail. 3rd close > 1st open. 3rd high < 2nd high
    
        df['cd_bees'] = np.where(  # Candlestick #1
                    (   # First < .5% increase
                        (df['changePercent'] > .005)
                    &   # 1st high < 2nd low
                        (df['fHigh'] < df['fLow'].shift(-1, axis=0))
                ),
                np.where(  # Candlestick #2
                    (   # Second > -.5 decrease, < -.15 decrease
                        (df['changePercent'].shift(-1, axis=0).between(
                            -.005, -.0015
                        ))
                    &   # Top tail < bottom tail
                        ((df['fHigh'].shift(-1, axis=0) - 
                          df['fOpen'].shift(-1, axis=0)) <
                         (df['fClose'].shift(-1, axis=0) -
                          df['fLow'].shift(-1, axis=0)))
                ),
               np.where(  # Candlestick #3
                    (   # Third < -.5% decrease
                        (df['changePercent'].shift(-2, axis=0) < -.005)
                    &   # 3rd close > 1st open
                        (df['fClose'].shift(-2, axis=0) >
                         df['fOpen'])

                    )
            , -1, 0), 0), 0)

        # print(df['cd_bees'].value_counts())
        return df
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    