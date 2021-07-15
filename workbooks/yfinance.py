# %% codecell
##########################################

import yfinance as yf
import pandas as pd

from multiuse.help_class import getDate

# %% codecell
##########################################

symbol = 'OCGN'
ticker = yf.Ticker(symbol)
ticker.options

options = ticker.option_chain('2021-06-25')
options
# %% codecell
##########################################
