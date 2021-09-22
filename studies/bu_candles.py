import pandas as pd
import numpy as np

def bu_prints(df):
    """Bullish column definitions and date printouts."""
    # Create function attribute that can be called outside scope
    bu_prints.bull_candles = {
    'cd_bumaru': {'column': 'cd_bumaru', 'name': 'Bullish Marubuzo', 'candle': 1},
    'cd_budoji': {'column': 'cd_budoji', 'name': 'Bullish Doji', 'candle': 1},
    'cd_bust': {'column': 'cd_bust', 'name': 'Bullish Spinning Top', 'candle': 1},
    'cd_budd': {'column': 'cd_budd', 'name': 'Bullish Dragonfly Doji', 'candle': 1},
    'cd_buiham': {'column': 'cd_buiham', 'name': 'Bullish Inverted Hammer', 'candle': 1},
    'cd_buham': {'column': 'cd_buham', 'name': 'Bullish Hammer', 'candle': 1},
    'cd_bukick': {'column': 'cd_bukick', 'name': 'Bullish Kicker', 'candle': 2},
    'cd_bugulf': {'column': 'cd_bugulf', 'name': 'Bullish Engulfing', 'candle': 2},
    'cd_buharam' : {'column': 'cd_buharam', 'name': 'Bullish Harami', 'candle': 2},
    'cd_bupline': {'column': 'cd_bupline', 'name': 'Bullish Piercing Line', 'candle': 2},
    'cd_butbot': {'column': 'cd_butbot', 'name' : 'Bullish Tweezer Bottom', 'candle': 2},
    'cd_bumorn': {'column': 'cd_bumorn', 'name' : 'Bullish Morning Star', 'candle': 3},
    'cd_buababy': {'column': 'cd_buababy', 'name': 'Bullish Abandoned Baby', 'candle': 3},
    'cd_butws':  {'column': 'cd_butws', 'name' : 'Bullish 3 White Soldiers', 'candle': 3},
    'cd_butls': {'column': 'cd_butls', 'name': 'Bullish 3 Line Strike', 'candle': 4},
    'cd_bumsd': {'column': 'cd_bumsd', 'name': 'Bullish Morning Star Doji', 'candle': 3},
    'cd_butou': {'column': 'cd_butou', 'name': 'Bullish 3 Outside Up', 'candle': 3},
    'cd_butiu': {'column': 'cd_butiu', 'name': 'Bullish 3 Inside Up', 'candle': 3}
             }

    getBuCandles = buCandles()

    for bu in bu_prints.bull_candles:
        # print(str(bu)[3:])
        eval(f"getBuCandles.{str(bu)[3:]}")(df)

    for bu in bu_prints.bull_candles:
        print(bu_prints.bull_candles[bu]['name'])
        print('##################################################')
        print(df[df[bu] == 1]['date'])
        print()


class buCandles():
    """Candlestick analysis 1-2-3 day."""

    @staticmethod
    def bumaru(df):
        """Bullish Marubuzo."""
        # Marubozo - Single candle. No top or bottom shadow.
        # Low and open should be the same, top and close should be the same.
        marubozo_cutoff = .1
        maru_inv = 1 - marubozo_cutoff  # Inverse

        df['cd_bumaru'] = np.where(
            (
                df['fLow'].between((df['fOpen'] * maru_inv), df['fOpen'])
                &
                df['fHigh'].between((df['fClose'] * maru_inv), df['fClose'])
            ), 1, 0)

        # # print(df['cd_bumaru'].value_counts())
        return df

    @staticmethod
    def budoji(df):
        """Bullish doji."""
        # Doji Candle - single candle
        # top and bottom shadows - open and close are relatively the same

        doji_cutoff = .01  # .025
        doji_tail_cutoff = .25  # .25 of the range for that day
        doji_inv = 1 - doji_cutoff

        df['cd_budoji'] = np.where(
                        (   # If close is within open +/- (range * .01)
                            df['fClose'].between((
                                df['fOpen'] - (df['fRange'] * .01)),
                               (df['fOpen'] + (df['fRange'] * .01))
                            )
                        &   # High > open + (range * .25)
                            df['fHigh'].where(
                                df['fHigh'] >
                               (df['fOpen'] +
                               (df['fRange'] * .25))
                            )
                        &   # Low < open - (range * .25)
                            df['fLow'].where(
                                df['fLow'] <
                               (df['fOpen'] -
                               (df['fRange'] * .25))
                            )
                        ), 1, 0)

        # # print(df['cd_budoji'].value_counts())
        return df


    @staticmethod
    def bust(df):
        """Bullish spinning top."""
        # Bullish spinning top - BUST - single candle
        df['cd_bust'] = np.where(
                                (   # Close between open +/- (range * .2)
                                    df['fClose'].between(
                                        (df['fOpen'] - (df['fRange'] * .20)),
                                        (df['fOpen'] + (df['fRange'] * .20))
                                    )
                                &   # Close > open
                                    (df['fClose'] > df['fOpen'])
                                &   # High > open + (range * .40)
                                    df['fHigh'].where(
                                        df['fHigh'] >
                                       (df['fOpen'] +
                                       (df['fRange'] * .40))
                                    )
                                &   # Low < open - (range * .40)
                                    df['fLow'].where(
                                        df['fLow'] <
                                       (df['fOpen'] - (df['fRange'] * .40))
                                    )
                                ), 1, 0)

        # # print(df['cd_bust'].value_counts())
        return df


    @staticmethod
    def budd(df):
        """Bullish Dragonfly Doji."""
        # Open and close the same, no top shadow, long bottom shadow
        budd_top_tail_cutoff = .15
        budd_bot_tail_cutoff = .50

        df['cd_budd'] = np.where(
                                (   # Close between open +/- (range * .05)
                                    df['fClose'].between(
                                        (df['fOpen'] - (df['fRange'] * .05)),
                                        (df['fOpen'] + (df['fRange'] * .05))
                                    )
                                &   # High < open + (range * .15)
                                    df['fHigh'].where(
                                         df['fHigh'] <
                                        (df['fOpen'] +
                                        (df['fRange'] * .15))
                                    )
                                &   # Low < open - (range * .50)
                                    df['fLow'].where(
                                         df['fLow'] <
                                        (df['fOpen'] -
                                        (df['fRange'] * .50))
                                    )
                                ), 1, 0)

        # # print(df['cd_budd'].value_counts())
        return df


    @staticmethod
    def buiham(df):
        """Bullish Inverted Hammer."""
        # Close above open, big top shadow, no bottom shadow
        df['cd_buiham'] = np.where(
                                (   # Close between open, open + (range * .25)
                                    df['fClose'].between(
                                        (df['fOpen']),
                                        (df['fOpen'] +
                                        (df['fRange'] * .25))
                                    )
                                &   # Close > open
                                   (df['fClose'] > df['fOpen'])
                                &   # High > open + (range * .75)
                                    df['fHigh'].where(
                                        df['fHigh'] >
                                       (df['fOpen'] +
                                       (df['fRange'] * .75))
                                    )
                                &   # Low > open - (range * .05)
                                    df['fLow'].where(
                                        df['fLow'] >
                                       (df['fOpen'] -
                                       (df['fRange'] * .05))
                                    )
                                ), 1, 0)

        # print(df['cd_buiham'].value_counts())
        return df


    @staticmethod
    def buham(df):
        """Bullish Hammer."""
        # Close above open, no top shadow, big bottom shadow
        df['cd_buham'] = np.where(
                        (   # If close is within open price, open price + .25 range
                            df['fClose'].between(
                                (df['fOpen']),
                                (df['fOpen'] + (df['fRange'] * .25))
                            )
                        &   # Close > open
                           (df['fClose'] > df['fOpen'])
                        &   # High < open + (range * .10)
                            df['fHigh'].where(
                                df['fHigh'] <
                               (df['fOpen'] +
                               (df['fRange'] * .10))
                            )
                        &   # Low < open - (range * .75)
                            df['fLow'].where(
                                df['fLow'] <
                               (df['fOpen'] - (df['fRange'] * .75))
                            )
                        ), 1, 0)

        # print(df['cd_buham'].value_counts())
        return df


    @staticmethod
    def bukick(df):
        """Bullish Kicker."""
        # 1st candle big decline, small shadows. 2nd candle big incline, small shadows
        # 2nd candle conditions need to be fixed
        df['cd_bukick'] = np.where(  # Candlestick #1
                        (       # Change < -.5% decrease
                                (df['changePercent'] < -.005)
                            &   # Close between low, open - (range * .4)
                                df['fClose'].between(
                                    (df['fLow']),
                                    (df['fOpen'] -
                                    (df['fRange'] * .4))
                                )
                            &   # High < open + (range * .2)
                                (df['fHigh'] <
                                (df['fOpen'] +
                                (df['fRange'] * .2)))
                            &   # Low > close - (range * .2)
                                (df['fLow'] >
                                (df['fClose'] -
                                (df['fRange'] * .2)))
                            &   # Check if the 1st and 2nd have the same symbol
                                (df['sym'] == (df['sym'].shift(-1, axis=0)))
                        ),

                        np.where(  # Candlestick #2
                        (      # Needs to move at least .5% positive
                                (df['changePercent'].shift(-1, axis=0) > .005)
                            &  # 2nd candle close between 1st open + (range * .5), 2nd close
                                df['fClose'].shift(-1, axis=0).between(
                                    (df['fOpen'] + (df['fRange'] * .5)),
                                    (df['fClose'].shift(-1, axis=0))
                                )
                            &  # 2nd open > (1st open - (1st range * .9)
                               (df['fOpen'].shift(-1, axis=0) >
                               (df['fOpen'] - (df['fRange'] * .90)))
                        )
                      , 1, 0), 0)

        # print(df['cd_bukick'].value_counts())
        return df


    @staticmethod
    def bugulf(df):
        """Bullish Engulfing."""
        #  1st candle decline with medium downward tail.
        #  2nd candle incline with medium downward tail. Open < 1st close, Close > 1st open

        df['cd_bugulf'] = np.where(  # Candlestick #1
                (   # First > .5% decrease
                    (df['changePercent'] < -.005)
                &   # If the candlestick closed within the ...
                    (df['fClose'].between(
                        (df['fLow'] + (df['fRange'] * .30)),
                        (df['fLow'] + (df['fRange'] * .80))
                    ))
                &   # If the bottom tail is less than 1/3 of the range
                    ((df['fClose'] - df['fLow']) <=
                          (df['fRange'] * .33))
                &   # Check if the 1st and 2nd have the same symbol
                    (df['sym'] == (df['sym'].shift(-1, axis=0)))
            ),

            np.where(
                (   # Second > .5% increase
                    (df['changePercent'].shift(-1, axis=0) > .005)
                &   # 2nd open is less than the 1st close
                    ((df['fOpen'].shift(-1, axis=0) -
                     (df['fRange'].shift(-1, axis=0) * .33)) <
                     df['fClose'])
                &   # 2nd close is greater than the 1st open
                    (df['fClose'].shift(-1, axis=0) > df['fOpen'] )
                &   # 2nd tail is less than 1/3 of the range
                    ((df['fLow'].shift(-1, axis=0) -
                      df['fOpen'].shift(-1, axis=0))  <=
                     (df['fRange'].shift(-1, axis=0) * 33)
                    )
            )
           , 1, 0), 0)

        # print(df['cd_bugulf'].value_counts())
        return df


    @staticmethod
    def buharam(df):
        """Bullish Harami."""
        # 1st candle huge decline with medium downward tail.
        # 2nd candle incline with equal tails. Open > 1st close, Close < 1st open

        df['cd_buharam'] = np.where(  # Candlestick #1
                (   # First < -.05% decrease
                    (df['changePercent'] < -.005)
                &   # If the candlestick closed within the ...
                    (df['fClose'].between(
                        (df['fLow'] + (df['fRange'] * .10)),
                        (df['fLow'] + (df['fRange'] * .45))
                    ))
                &   # If the bottom tail is less than 1/3 of the range
                    ((df['fClose'] -
                      df['fLow']) <=
                     (df['fRange'] * .33)
                    )
                &   # Check if the 1st and 2nd have the same symbol
                    (df['sym'] == (df['sym'].shift(-1, axis=0)))
            ),

            np.where(  # Candlestick #2
                (   # Second > .5% increase
                    (df['changePercent'].shift(-1, axis=0) > .01)
                &   # 2nd open is less than the 1st close
                   ((df['fOpen'].shift(-1, axis=0) +
                    (df['fRange'].shift(-1, axis=0) * .15)) >
                     df['fClose'])
                &   # 2nd close < the 1st open
                    (df['fClose'].shift(-1, axis=0) <
                     df['fOpen'])
                &   # 2nd close within 20% of range from the 1st open
                    (df['fClose'].shift(-1, axis=0).between(
                        (df['fOpen'] - (df['fRange'] * .15)),
                        (df['fOpen'])
                    ))
                &   # 2nd top tail < 25% of the range
                   ((df['fHigh'].shift(-1, axis=0) -
                     df['fClose'].shift(-1, axis=0)) <=
                    (df['fRange'].shift(-1, axis=0) * .25)
                    )
                &   # 2nd bottom tail < 25% of the range
                   ((df['fLow'].shift(-1, axis=0) -
                     df['fOpen'].shift(-1, axis=0)) <=
                    (df['fRange'].shift(-1, axis=0) * .25)
                   )
            )
           , 1, 0), 0)

        # print(df['cd_buharam'].value_counts())
        return df


    @staticmethod
    def bupline(df):
        """Bullish Piercing Line."""
        # 1st candle decline with m downward tail, ms upward tail
        # 2nd candle incline with m tails. Open < 1st close, Close ~ .5 body of 1st

        df['cd_bupline'] = np.where(  # Candlestick #1
                (   # First < .5% decrease
                    (df['changePercent'] < -.005)
                &   # If the candlestick closed within the ...
                    (df['fClose'].between(
                        (df['fLow'] + (df['fRange'] * .10)),
                        (df['fLow'] + (df['fRange'] * .60))
                    ))
                &   # If the bottom tail is less than .5 of the range
                    ((df['fClose'] - df['fLow']) <=
                     (df['fRange'] * .5)
                    )
                &   # If the bottom tail > top tail
                    (((df['fClose'] - df['fLow'])) >=
                      (df['fHigh'] - df['fOpen'])
                    )
                &   # If the close is within .33 of range of 2nd midpoint
                    (df['fClose'].between(
                       (((df['fClose'].shift(-1, axis=0) -
                          df['fOpen'].shift(-1, axis=0)) / 2) -
                         (df['fRange'].shift(-1, axis=0) * .33)
                       ),
                       (((df['fClose'].shift(-1, axis=0) -
                          df['fOpen'].shift(-1, axis=0)) / 2) +
                         (df['fRange'].shift(-1, axis=0) * .33)
                       )
                    ))
                &   # Check if the 1st and 2nd have the same symbol
                    (df['sym'] == (df['sym'].shift(-1, axis=0)))
            ),
            np.where(  # Candlestick #2
                (   # Second > .5% increase
                     (df['changePercent'].shift(-1, axis=0) > .005)
                &   # 2nd open is less than the 1st low
                    ((df['fOpen'].shift(-1, axis=0) <
                      df['fLow'])
                    )
                &   # 2nd close > than the 1st close + .1 range
                    ((df['fClose'].shift(-1, axis=0)) >
                     (df['fClose'] + (df['fRange'] * .1))
                    )
            )
           , 1, 0), 0)

        # print(df['cd_bupline'].value_counts())
        return df


    @staticmethod
    def butbot(df):
        """Bullish Tweezer Bottom."""
        #  1st candle decline with s tails. Bot tail ~ Bot tail 2nd candle
        #  2nd candle incline with top tail less than 1st open. Open < 1st close. Bottom tail > top tail

        df['cd_butbot'] = np.where(  # Candlestick #1
                (   # First > .5% decrease
                    (df['changePercent'] < -.005)
                &   # If the candlestick closed within the ...
                    (df['fClose'].between(
                        (df['fLow'] + (df['fRange'] * .05)),
                        (df['fLow'] + (df['fRange'] * .40))
                    ))
                &   # If the bottom tail is less than 1/5 of the range
                   ((df['fClose'] - df['fLow']) <=
                    (df['fRange'] * .20)
                    )
                &   # If the close is within .25 of range of 2nd midpoint
                    (df['fClose'].between(
                          (df['fLow'].shift(-1, axis=0)),
                          (df['fClose'].shift(-1, axis=0))
                    ))
                &   # Check if the 1st and 2nd have the same symbol
                    (df['sym'] == (df['sym'].shift(-1, axis=0)))
            ),
            np.where(  # Candlestick #2
                (   # Second > .5% increase
                     (df['changePercent'].shift(-1, axis=0) > .005)
                &   # 2nd open is greater than the 1st close
                    ((df['fOpen'].shift(-1, axis=0) >
                      df['fClose']))
                &   # 2nd high < than the 1st open
                    ((df['fHigh'].shift(-1, axis=0)) <
                     (df['fOpen']))
                &   # Top tail < bottom tail
                    ((df['fHigh'].shift(-1, axis=0) -
                      df['fClose'].shift(-1, axis=0)) <=
                     (df['fOpen'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0))
                    )
            )
           , 1, 0), 0)

        # print(df['cd_butbot'].value_counts())
        return df


    @staticmethod
    def bumorn(df):
        """Bullish Morning Star."""
        # Bullish Morningstar - BUMORN -
        # 1st candle - Big red candle, m bottom tail, ss top tail
        # 2nd candle - Small red/green candle, s top tail, ms bottom tail
        # 3rd candle - Big green candle, m bottom tail, ss top tail

        df['cd_bumorn'] = np.where(  # Candlestick #1
                (   # First < -.5% decrease
                    (df['changePercent'] < -.005)
                &   # If the candlestick closed within the ...
                    (df['fClose'].between(
                        (df['fLow'] + (df['fRange'] * .05)),
                        (df['fLow'] + (df['fRange'] * .40))
                    ))
                &   # Check if the 1st and 3rd have the same symbol
                    (df['sym'] == (df['sym'].shift(-2, axis=0)))
                &   # Candle body takes up at least 50% of the range
                    (((df['fOpen'] - df['fClose']) / df['fRange']) > .5)

            ),
            np.where(  # Candlestick #2
                (   # Second > -2.5% decrease, < 2.5% increase
                    (df['changePercent'].shift(-1, axis=0).between(
                        - .025, .025
                    ))
                &   # 2nd open < the 1st low
                    (df['fOpen'].shift(-1, axis=0) <
                     df['fLow'])
                &   # 2nd high < 1st low
                    (df['fHigh'].shift(-1, axis=0) <
                     df['fLow'])
            ),
           np.where(  # Candlestick #3
                (   # Third > .5% increase
                    (df['changePercent'].shift(-2, axis=0) > .005)
                &   # Third close > third open
                    (df['fClose'].shift(-2, axis=0) >
                     df['fOpen'].shift(-2, axis=0))
                &   # 3rd open > 2nd low + .2 range
                    (df['fOpen'].shift(-2, axis=0) >
                       (df['fLow'].shift(-1, axis=0) +
                       (df['fRange'].shift(-1, axis=0) * .2))
                    )
                &   # 3rd close > (1st close + (range of 2nd * .5))
                    ((df['fClose'].shift(-2, axis=0)) >
                     (df['fClose'] + (df['fRange'].shift(-1, axis=0) * .5)
                    ))
                &   # 3rd close > 2nd high
                    (df['fClose'].shift(-2, axis=0) >
                     df['fHigh'].shift(-1, axis=0))

                )
        , 1, 0), 0), 0)

        # print(df['cd_bumorn'].value_counts())
        return df


    @staticmethod
    def buababy(df):
        """Bullish Abandoned Baby."""
        # Bullish Abandoned Baby - BUABABY -
        # 1st candle - Big red candle, m bottom tail, m top tail
        # 2nd candle - Smaller green candle, s top tail, m bottom tail
        # 3rd candle - Big green candle, m bottom tail, m top tail. Close ~ 1st open

        df['cd_buababy'] = np.where(  # Candlestick #1
                (   # First < -.5% decrease
                    (df['changePercent'] < -.005)
                &   # 1st close < 1st open
                    (df['fClose'] < df['fOpen'])
                # &   # If the candlestick closed within the ...
                #    (df['fClose'].between(
                #        (df['fLow'] + (df['fRange'] * .10)),
                #        (df['fLow'] + (df['fRange'] * .50))
                #    ))
                &   # Candle body > .5 of candle range
                    (((df['fOpen'] - df['fClose']) / df['fRange']) > .5)
                &   # 1st close < 3rd close
                    (df['fClose'] < df['fClose'].shift(-2, axis=0))
                &   # Check if the 1st and 3rd have the same symbol
                    (df['sym'] == (df['sym'].shift(-2, axis=0)))
            ),
            np.where(  # Candlestick #2
                (   # Second < .1% decrease
                    (df['changePercent'].shift(-1, axis=0) < -.01)
                &   # Second close > 2nd open
                    (df['fClose'].shift(-1, axis=0) >
                     df['fOpen'].shift(-1, axis=0))
                &   # 2nd high < 1st low
                    (df['fHigh'].shift(-1, axis=0) < df['fLow'])
            ),
           np.where(  # Candlestick #3
                (   # Third > 1.5% increase
                    (df['changePercent'].shift(-2, axis=0) > .015)
                &   # Third close > third open
                    (df['fClose'].shift(-2, axis=0) >
                     df['fOpen'].shift(-2, axis=0))
                &   # Third low > 2nd high
                    (df['fLow'].shift(-2, axis=0) >
                     df['fHigh'].shift(-1, axis=0))
                &   # Third close > 1st close
                    (df['fClose'].shift(-2, axis=0) >
                     df['fClose'])
                # &   # Third range > .5 * 1st range
                #    (df['fRange'].shift(-2, axis=0) > (.5 * df['fRange']))
                # &   # 3rd close within .3 range of 1st open +/-
                #    (df['fClose'].shift(-2, axis=0).between(
                #     (df['fOpen'] - (df['fRange'] * .3)),
                #     (df['fOpen'] + (df['fRange'] * .3))
                #    ))

                )
        , 1, 0), 0), 0)

        # print(df['cd_buababy'].value_counts())
        return df


    @staticmethod
    def butws(df):
        """Bullish 3 White Soldiers."""
        # Bullish 3 White Soldiers - BUTWS -
        # 1st candle - Medium green candle. Close > 2nd close. S tail
        # 2nd candle - Medium green candle. Close > 3rd close. S tail
        # 3rd candle - Medium green candle. S tail

        df['cd_butws'] = np.where(  # Candlestick #1
                (   # First > .25% increase
                    (df['changePercent'] > .0025)
                &   # 1st close < 2nd close
                    (df['fClose'] < df['fClose'].shift(-1, axis=0))
                &   # Bottom tail < .3 range
                    ((df['fOpen'] - df['fLow']) <
                     (df['fRange']) * .3)
                &   # Top tail < .3 range
                    ((df['fHigh'] - df['fClose']) <
                     (df['fRange']) * .3)
                &   # Check if the 1st and 3rd have the same symbol
                    (df['sym'] == (df['sym'].shift(-2, axis=0)))
            ),
            np.where(  # Candlestick #2
                (   # Second > .25% increase
                    (df['changePercent'].shift(-1, axis=0) > .0025)
                &   # 2nd close < 3rd close
                    (df['fClose'].shift(-1, axis=0) <
                     df['fClose'].shift(-2, axis=0))
                &   # Bottom tail < .3 range
                    ((df['fOpen'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0)) <
                     (df['fRange'].shift(-1, axis=0)) * .3)
                &   # Top tail < .3 range
                    ((df['fHigh'].shift(-1, axis=0) -
                      df['fClose'].shift(-1, axis=0)) <
                     (df['fRange'].shift(-1, axis=0)) * .3)
            ),
           np.where(  # Candlestick #3
                (   # Third > .25% increase
                    (df['changePercent'].shift(-2, axis=0) > .0025)
                &   # Open < 2nd close
                    (df['fOpen'].shift(-2, axis=0) <
                     df['fClose'].shift(-1, axis=0))
                &   # Bottom tail < .3 range
                    ((df['fOpen'].shift(-2, axis=0) -
                      df['fLow'].shift(-2, axis=0)) <
                     (df['fRange'].shift(-2, axis=0)) * .3)
                &   # Top tail < .3 range
                    ((df['fHigh'].shift(-2, axis=0) -
                      df['fClose'].shift(-2, axis=0)) <
                     (df['fRange'].shift(-2, axis=0)) * .3)
                )
            , 1, 0), 0), 0)

        # print(df['cd_butws'].value_counts())
        return df


    @staticmethod
    def butls(df):
        """Bullish 3 Line Strike."""
        # Bullish 3 Line Strike - BUTLS - 4 candles
        # 1st candle - Small red candle. 1st open > 2nd open
        # 2nd candle - Small red candle. 2nd open > 3rd open. 2nd close < 1st close.
        # 3rd candle - Small red candle. 3rd close < 2nd close.
        # 4th canlde - Big green candle. 4th open < 3rd close. 4th close > 1st open

        df['cd_butls'] = np.where(  # Candlestick #1
                (   # First < -.25% decrease
                    (df['changePercent'] < -.0025)
                &   # 1st close < 1st open
                    (df['fClose'] < df['fOpen'])
                &   # 1st open > 2nd open
                    (df['fOpen'] > df['fOpen'].shift(-1, axis=0))
                &   # Check if the 1st and 4th have the same symbol
                    (df['sym'] == (df['sym'].shift(-3, axis=0)))
            ),
            np.where(  # Candlestick #2
                (   # Second < -.25% decrease
                    (df['changePercent'].shift(-1, axis=0) < -.0025)
                &   # 2nd close < 2nd open
                    (df['fClose'].shift(-1, axis=0) <
                     df['fOpen'].shift(-1, axis=0))
                &   # 2nd open > 3nd open
                    (df['fOpen'].shift(-1, axis=0) >
                     df['fOpen'].shift(-2, axis=0))
                &   # 2nd close < 1st close
                    (df['fClose'].shift(-1, axis=0) <
                     df['fClose'])
            ),
           np.where(  # Candlestick #3
                (   # Third < -.25% decrease
                    (df['changePercent'].shift(-2, axis=0) < -.0025)
                &   # Third close < third open
                    (df['fClose'].shift(-2, axis=0) <
                     df['fOpen'].shift(-2, axis=0))
                &   # 3rd close < 3rd open
                    (df['fClose'].shift(-2, axis=0) <
                     df['fOpen'].shift(-2, axis=0))
                &   # 3rd close < 2nd close
                    (df['fClose'].shift(-2, axis=0) <
                     df['fClose'].shift(-1, axis=0))
                ),
           np.where(  # Candlestick #4
                (   # 4th > 1% increase
                    (df['changePercent'].shift(-3, axis=0) > .01)
                &   # 4th close > 4th open
                    (df['fClose'].shift(-3, axis=0) >
                     df['fOpen'].shift(-3, axis=0))
                &   # 4th open < 3rd close
                    (df['fOpen'].shift(-3, axis=0) <
                     df['fClose'].shift(-2, axis=0))
                &   # 4th close > 1st open
                    (df['fClose'].shift(-3, axis=0) > df['fOpen'])
                &   # Top tail < 20% of the 4th range
                    (((df['fHigh'].shift(-3, axis=0) -
                       df['fClose'].shift(-3, axis=0)) /
                       df['fRange'].shift(-3, axis=0)) < .2)
                &   # Bottom tail < 20% of the 4th range
                    (((df['fOpen'].shift(-3, axis=0) -
                       df['fLow'].shift(-3, axis=0)) /
                       df['fRange'].shift(-3, axis=0)) < .2)

                )
        , 1, 0), 0), 0), 0)

        # print(df['cd_butls'].value_counts())
        return df


    @staticmethod
    def bumsd(df):
        """Bullish Morning Star Doji."""
        # Bullish Morning Star Doji - BUMSD -
        # 1st candle - Big red candle. SS tails
        # 2nd candle - Doji with m tails. Open/close ~ 1st close and 3rd open
        # 3rd candle - Medium green candle. S tail. 3rd close < 1st open. 3rd high < 1st open.

        df['cd_bumsd'] = np.where(  # Candlestick #1
                (   # First < -.5% increase
                    (df['changePercent'] < -.005)
                &   # 1st close < 2nd high
                    (df['fClose'] < df['fHigh'].shift(-1, axis=0))
                &   # Check if the 1st and 3rd have the same symbol
                    (df['sym'] == (df['sym'].shift(-2, axis=0)))
                &   # 1st body > .67 of candle range
                    ((abs(df['fOpen'] - df['fClose']) /
                          df['fRange']) > .67)
            ),
            np.where(  # Candlestick #2
                (   # Second < .33% increase, > -.33 decrease
                    (df['changePercent'].shift(-1, axis=0).between(
                        -.0033, .0033
                    ))
                &   # Bottom tail > .3 range
                    ((df['fOpen'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0)) >
                     (df['fRange'].shift(-1, axis=0)) * .3)
                &   # Top tail > .3 range
                    ((df['fHigh'].shift(-1, axis=0) -
                      df['fClose'].shift(-1, axis=0)) >
                     (df['fRange'].shift(-1, axis=0)) * .3)
                &   # 2nd high < 1st open
                    (df['fHigh'].shift(-1, axis=0) < df['fOpen'])
                &   # 2nd high < 3rd close
                    (df['fHigh'].shift(-1, axis=0) <
                     df['fClose'].shift(-2, axis=0))
                &   # 2nd body < .5 of candle range
                    ((abs(df['fOpen'].shift(-1, axis=0) -
                          df['fClose'].shift(-1, axis=0)) /
                          df['fRange'].shift(-1, axis=0)
                      ) < .5)
            ),
           np.where(  # Candlestick #3
                (   # Third > .5% increase
                    (df['changePercent'].shift(-2, axis=0) > .005)
                &   # Third close > third open
                    (df['fClose'].shift(-2, axis=0) >
                     df['fOpen'].shift(-2, axis=0))
                &   # 3rd close < 1st open
                    (df['fClose'].shift(-2, axis=0) <
                     df['fOpen'])
                &   # 3rd body > .67 of candle range
                    ((abs(df['fOpen'].shift(-2, axis=0) -
                          df['fClose'].shift(-2, axis=0)) /
                          df['fRange'].shift(-2, axis=0)
                      ) > .67)
                )
        , 1, 0), 0), 0)

        # print(df['cd_bumsd'].value_counts())
        return df


    @staticmethod
    def butou(df):
        """Bullish 3 Outside Up."""
        # Bullish Three Outside Up - BUTOU -
        # 1st candle - Medium red candle. M bottom tail. SS top tail.
        # 2nd candle - Big green candle. 2nd open < 1st close. 2nd close > 1st open. S tails.
        # 3rd candle - Medium green candle. Open ~ 1st open. Close > 2nd close. M tails.

        df['cd_butou'] = np.where(  # Candlestick #1
                (   # First < -.25% decrease. > -1.5% decrease
                    (df['changePercent'].between(
                        -.05, -.0025
                    ))
                &   # Open above close
                    (df['fOpen'] > df['fClose'])
                &   # Bottom tail > top tail
                    ((df['fClose'] - df['fLow']) >
                     (df['fHigh'] - df['fOpen']))
                &   # First low should be > than 2nd low
                    ((df['fLow'] > df['fLow'].shift(-1, axis=0)))
                &   # First low should be higher than 2nd open
                    (df['fLow'] > df['fOpen'].shift(-1, axis=0))
                &   # Check if the 1st and 3rd have the same symbol
                    (df['sym'] == (df['sym'].shift(-2, axis=0)))
            ),
            np.where(  # Candlestick #2
                (   # Second > .5% increase
                    (df['changePercent'].shift(-1, axis=0) > .005)
                &   # 2nd high greater than first high
                    (df['fHigh'].shift(-1, axis=0) >
                     df['fHigh'])
                &   # Second close > 2nd open
                    (df['fClose'].shift(-1, axis=0) >
                     df['fOpen'].shift(-1, axis=0))
                &   # 2nd close > 1st open
                    (df['fClose'].shift(-1, axis=0) >
                     df['fOpen'])
                &   # 2nd open < 1st close
                    (df['fOpen'].shift(-1, axis=0) <
                     df['fClose'])
                &   # 3rd top tail less than 20% of candle body
                    ((df['fHigh'].shift(-1, axis=0) -
                      df['fClose'].shift(-1, axis=0)) <
                     (.2 * (df['fHigh'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0))))
                &   # 3rd bottom tail less than 20% of candle body
                    ((df['fOpen'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0)) <
                     (.2 * (df['fHigh'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0))))
            ),
           np.where(  # Candlestick #3
                (   # Third > .33% increase
                    (df['changePercent'].shift(-2, axis=0) > .0033)
                &   # Third close > third open
                    (df['fClose'].shift(-2, axis=0) >
                     df['fOpen'].shift(-2, axis=0))
                &   # 3rd close > 2nd close
                    (df['fClose'].shift(-2, axis=0) >
                     df['fClose'].shift(-1, axis=0))
                &   # 3rd open < 2nd close
                    (df['fOpen'].shift(-2, axis=0) <
                     df['fClose'].shift(-1, axis=0))
                &   # 3rd low > 2nd low
                    (df['fLow'].shift(-2, axis=0) >
                     df['fLow'].shift(-1, axis=0))
                &   # 3rd top tail less than 33% of candle body
                    ((df['fHigh'].shift(-2, axis=0) -
                      df['fClose'].shift(-2, axis=0)) <
                     (.33 * (df['fHigh'].shift(-2, axis=0) -
                      df['fLow'].shift(-2, axis=0))))
                &   # 3rd bottom tail less than 33% of candle body
                    ((df['fOpen'].shift(-2, axis=0) -
                      df['fLow'].shift(-2, axis=0)) <
                     (.33 * (df['fHigh'].shift(-2, axis=0) -
                      df['fLow'].shift(-2, axis=0))))
                )
        , 1, 0), 0), 0)

        # print(df['cd_butou'].value_counts())
        return df


    @staticmethod
    def butiu(df):
        """Bullish 3 Inside Up."""
        # Bullish Three Inside Up - BUTIU -
        # 1st candle - Big red candle. Bottom tail > top tail
        # 2nd candle - Medium green candle. Open > 1st close < 1st open. Close < 1st open, > 1st close
        # 3rd candle - Big green candle. Open > 2nd open. Close > 2nd open, 1st open

        df['cd_butiu'] = np.where(  # Candlestick #1
                (   # First < -.5% decrease
                    (df['changePercent'] < -.005)
                &   # Bottom tail > top tail
                    ((df['fClose'] - df['fLow']) >
                     (df['fHigh'] - df['fOpen']))
                &   # Check if the 1st and 3rd have the same symbol
                    (df['sym'] == (df['sym'].shift(-2, axis=0)))
            ),
            np.where(  # Candlestick #2
                (   # Second > .25% increase
                    (df['changePercent'].shift(-1, axis=0) > .0025)
                &   # 2nd open > 1st close, 2nd open < 1st open
                    (df['fOpen'].shift(-1, axis=0).between(
                        df['fClose'], df['fOpen']
                    ))
                &   # 2nd open > 1st close, 2nd open < 1st close
                    (df['fClose'].shift(-1, axis=0).between(
                        df['fClose'], df['fOpen']
                    ))
                &   # Top tail < bottom tail
                    ((df['fClose'].shift(-1, axis=0) -
                      df['fHigh'].shift(-1, axis=0)) <
                     (df['fOpen'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0)))
                &   # 2nd bot tail < high - open
                    ((df['fOpen'].shift(-1, axis=0) -
                      df['fLow'].shift(-1, axis=0)) <
                     (df['fHigh'].shift(-1, axis=0) -
                      df['fOpen'].shift(-1, axis=0))
                    )
                &   # 2nd tail should be less than the first open
                    (df['fHigh'].shift(-1, axis=0) < df['fOpen'])

            ),
           np.where(  # Candlestick #3
                (   # Third > .5% increase
                    (df['changePercent'].shift(-2, axis=0) > .005)
                &   # Close > open
                    (df['fClose'].shift(-2, axis=0) > df['fOpen'].shift(-2, axis=0))
                &   # Open > 2nd open
                    (df['fOpen'].shift(-2, axis=0) >
                     df['fOpen'].shift(-1, axis=0))
                &   # Close > 1st open
                    (df['fClose'].shift(-2, axis=0) >
                     df['fOpen'])
                &   # 3rd top tail less than 20% of the candle body
                    ((df['fHigh'].shift(-2, axis=0) -
                      df['fClose'].shift(-2, axis=0)) <
                     ((df['fHigh'].shift(-2, axis=0) -
                      df['fLow'].shift(-2, axis=0)) * .20)
                    )
                &   # 3rd bottom tail less than 20% of the candle body
                    ((df['fOpen'].shift(-2, axis=0) -
                      df['fLow'].shift(-2, axis=0)) <
                     ((df['fHigh'].shift(-2, axis=0) -
                      df['fLow'].shift(-2, axis=0)) * .20)
                    )
                )

        , 1, 0), 0), 0)

        # print(df['cd_butiu'].value_counts())
        return df
