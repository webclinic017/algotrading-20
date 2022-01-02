"""Plotting stock data with matplotlib."""
# %% codecell
############################################

import pandas as pd
import matplotlib.pyplot as plt

# %% codecell
############################################
# df_ohlc = plot_cols(aapl_ma, vol=True, candle=True, moving_averages='cma')


def plot_cols(df, y='fClose', moving_averages=None, vol=False, candle=False, figsize=(16, 8)):
    """Plot stock data. y can be a list."""
    import matplotlib.pyplot as plt
    from matplotlib import style
    style.use('ggplot')

    cols = df.columns
    df = df.copy()

    if candle:
        import mplfinance as mpf
        from mplfinance.original_flavor import candlestick_ohlc
        import matplotlib.dates as mdates

    # Determine if the date column is datetime (it should be)
    # fClose is the default adjusted closing price
    ma_list = ['10', '20', '50', '200']

    try:
        if df['date'].dtype != '<M8[ns]':
            df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
        df['date_keys'] = df['date']
        df = df.set_index(keys='date')
    except KeyError:
        print('Could not find date column')
        pass

    if vol and not candle:
        fig = plt.figure(figsize=figsize)
        ax1 = plt.subplot2grid((7, 1), (0, 0), rowspan=4, colspan=1)
        ax2 = plt.subplot2grid((7, 1), (5, 0), rowspan=2, colspan=1, sharex=ax1)

        # ax1.plot(df.index, df[y], label='Close')
        df_index = df.index.get_level_values('date')

        df.plot(x='date_keys', y='fOpen', label='fOpen', kind='scatter', ax=ax1, color='black')
        df.plot(x='date_keys', y='fClose', label='fClose', kind='scatter', ax=ax1, color='blue')
        df.plot(x='date_keys', y='fHigh', label='fHigh', kind='scatter', ax=ax1, color='green')
        df.plot(x='date_keys', y='fLow', label='fLow', kind='scatter', ax=ax1, color='red')
        # print(dir(ax1))

        if 'volume' in cols:
            ax2.bar(df.index, df['volume'], label='Volume')
        elif 'fVolume' in cols:
            ax2.bar(df.index, df['vol/mil'], label='Volume (in millions)', color='Blue')

        if moving_averages:
            ma = moving_averages
            for m in ma_list:
                ax1.plot(df.index, df[f"{ma}_{m}"], label=f"{ma.upper()} {m}")

    elif not vol and not candle:
        df[y].plot(label='Close', figsize=figsize, grid=True)

        if moving_averages:
            ma = moving_averages
            df[f"{ma}_10"].plot(label=f"{ma.upper()} 10")
            df[f"{ma}_20"].plot(label=f"{ma.upper()} 20")
            df[f"{ma}_50"].plot(label=f"{ma.upper()} 50")
            df[f"{ma}_200"].plot(label=f"{ma.upper()} 200")

    elif vol and candle:

        # df_mod = df.reset_index().copy(deep=True)
        df_ohlc = df[['fOpen', 'fHigh', 'fLow', 'fClose']].copy(deep=True)
        df_ohlc.reset_index(inplace=True)
        df_ohlc['date'] = df_ohlc['date'].map(mdates.date2num)

        fig = plt.figure(figsize=figsize)
        ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=4, colspan=1)
        ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)
        ax1.xaxis_date()

        candlestick_ohlc(ax1, df_ohlc.values, colorup='g')

        if moving_averages:
            ma = moving_averages
            for m in ma_list:
                ax1.plot(df.index, df[f"{ma}_{m}"], label=f"{ma.upper()} {m}")

        # ax2.fill_between(df.index, df['volume'], 0)
        ax2.bar(df.index, df['volume'], label='Volume')

        return df_ohlc


    plt.legend()
