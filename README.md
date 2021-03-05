AlgoTrading:


Description:
Buy/Sell signal implementation using basic technical analysis, trending symbols from stocktwits, identifying unusual volume in the derivatives market.

	1. Data sources - OCC, CBOE, Stocktwits, IEX Cloud.
	2. first_pass.ipynb is the starting point for stock data
	3. Stocktwits.py gets trending symbols every 6 minutes.
	4. iex_routines.py gets a list of support symbols (OTC included), compares to the previous day, and identifies new symbols.
	5. theocc_class.py has classes for pulling volume in the FLEX exchange, as well as trade volume.
	6. cboe_class.py pulls market maker opportunity data in the derivatives market for 4 exchanges: Cboe, BZX, C2, and EDGX.


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
	- Stored in local, compressed json files. Will convert to faster file read/write format at some point down the line.

Resources:
	Creating Bins  - https://stackoverflow.com/questions/45273731/binning-column-with-python-pandas



To - Do:
	Data:
		- Set up data pipeline
		- Integrate cboe and theocc classes with underlying equity data.
		- Set up alpaca market streaming data (free).
		- Set up Interactive Brokers data stream.

	Candlesticks:
		- Test current implementations
		- https://www.alphaexcapital.com/candlestick-patterns/candlestick-pattern-cheat-sheet/

	And many other things that still need to be done!
