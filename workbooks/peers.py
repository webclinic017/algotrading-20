"""Peers list worbooks."""
# %% codecell
import sys
from pathlib import Path
import importlib

import pandas as pd
import numpy as np
from tqdm import tqdm

from multiuse.help_class import getDate, baseDir, write_to_parquet


from api import serverAPI
importlib.reload(sys.modules['api'])
from api import serverAPI

from daily_front_funcs.corr_peers import PeerList
importlib.reload(sys.modules['daily_front_funcs.corr_peers'])
from daily_front_funcs.corr_peers import PeerList

from workbooks_fib.fib_funcs import read_clean_combined_all

# %% codecell

pl = PeerList(filter_extremes=True)

# %% codecell

# %% codecell
from ref_data.symbol_meta_stats import SymbolRefMetaInfo
from multiuse.fpaths import FindTheFpath

# srmi = SymbolRefMetaInfo()
# %% codecell

fpath = FindTheFpath(category='peers', keyword='extremes')
df_test = pd.read_parquet(fpath.fpath)

# %% codecell

# %% codecell

df_test.loc['OCGN'].nlargest(5, 'corr')

df_test

# %% codecell

# There's the question of correlation with percentage returns
# Or whether to apply a logarithmic function to flatten the noise.
# I'm guess that ^ this is probably the better idea.

scaled_price = (logprice -np.mean(logprice))/np.sqrt(np.var(logprice))

# %% codecell
fpath = Path(baseDir().path, 'ref_data', 'peer_list', '_peers.parquet')
df_peers = pd.read_parquet(fpath)

all_syms = serverAPI('all_symbols').df
df_peers = pd.merge(df_peers, all_syms[['symbol', 'type']], on='symbol', how='left')
df_peers = (df_peers.mask('corr', .95, lesser=True)
                    .mask('corr', -.95, greater=True))

# %% codecell
df_peers_idx = df_peers.set_index(['key', 'type'])

df_peers

df_peers[df_peers['key'] == 'CYBN']

df_peers[df_peers['symbol'] == 'OCGN']

df_peers
df_peers.mask('key', 'OCGN')

# %% codecell
