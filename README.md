AlgoTrading:


Description:
Buy/Sell signal implementation using basic technical analysis, trending symbols from stocktwits, identifying unusual volume in the derivatives market.

	1. Data sources - OCC, CBOE, Stocktwits, IEX Cloud, SEC, Alpaca, Yahoo Finance, Nasdaq
	2. Website - https://algotrading.ventures - update/fix frontend functions


Moving Averages:
	- 10/20/50/200
	- Crossover signals
Studies:
	- RSI 14
	- OBV - On Balance Volume
	- A/D - Accumulation/Distribution

Candlesticks:
	- Bullish/Bearish 1/2/3 day candle patterns
	-

Database:
	- Stored in local, compressed json files.
	- Newer files are in parquet.




To-Do:
	Data:
		- Integrate with TD Ameritrade

	Candlesticks:
		- Test current implementations
		- https://www.alphaexcapital.com/candlestick-patterns/candlestick-pattern-cheat-sheet/
