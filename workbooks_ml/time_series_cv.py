"""Time series cross validation."""
# %% codecell


# %% codecell

tscv = TimeSeriesSplit(n_splits=5)
for train, validate in tscv.split(data):
    print(train, validate)

# %% codecell
