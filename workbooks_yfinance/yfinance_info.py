"""Yfinance info workbook."""
# %% codecell



# %% codecell

serverAPI('redo', val='combine_yfinance_info')

# %% codecell
serverAPI('redo', val='yfinance_info')

# %% codecell
SetUpYahooOptions(options=False, other='yinfo')

# %% codecell

df_info = serverAPI('yinfo_all').df

CachedSession
session = CachedSession('yfinance.cache')
session.proxies = proxy_0
proxy_0 = {'http': 'socks5://edwardtomasso@gmail.com:YNGWqBf2zHaLSV6@socks-nl1.nordvpn.com:1080'}

ocgn = yf.Ticker('ocgn', session=session)


ocgn.info
nord_proxies = get_sock5_nord_proxies()
nord_proxies[0]

# %% codecell

df_info.info()

# %% codecell
cols_to_drop = ['logo_url', 'zip', 'longBusinessSummary', 'phone', 'state', 'website', 'address1', 'address2', ]
df_info['symbol'].isna().sum()

cols_na_drop = []
for col in list(df_info.columns):
        if df_info[col].isna().sum() > (.85 * df_info.shape[0]):
            cols_na_drop.append(col)


df_not_na = df_info.drop(columns=cols_na_drop).dropna().copy()

df_info[df_info['symbol'] == 'AAPL'].info()
df_not_na.shape

# %% codecell
df_beta = df_info[~df_info['beta'].isna()].copy()
df_short = df_info[~df_info['sharesShortPreviousMonthDate'].isna()].copy()
df_short.shape


list(df_info.columns)
# %% codecell
