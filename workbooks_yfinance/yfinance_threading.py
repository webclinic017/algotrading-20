"""Threading example for yfinance."""
# %% codecell
import logging
import threading
import time
import concurrent.futures

# %% codecell

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(messae)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(thread_function, range(3))



# %% codecell

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

from tqdm import tqdm
def call_yahoo_options(df):
    """Call yahoo options with the right proxy."""
    # print(df)
    for index, row in tqdm(df.iterrows()):
        # print([row['symbol'], row['proxy']])
        try:
            yahoo_options(row['symbol'], proxy=row['proxy'])
        except Exception as e:
            print(e)
        # break


arg_list = ['flower', 'petal', 'creature']
args = [df_comb[df_comb['bins'] == n] for n in iter(bins)]

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    args = [df_comb[df_comb['bins'] == n] for n in iter(bins)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(call_yahoo_options, iter(args))
