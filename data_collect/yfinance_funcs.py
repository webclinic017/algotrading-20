"""Helper functions for handling yoptions/yfinance data."""
# %% codecell
from pathlib import Path
from io import BytesIO

import requests
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg

# %% codecell


def return_yoptions_temp_all():
    """Return dataframe of all yoptions temp (today's data)."""
    df_all = None

    # If local environment
    if 'Algo' in baseDir().path:
        try:
            from api import serverAPI
            df_all = serverAPI('yoptions_temp').df
        except ModuleNotFoundError as me:
            help_print_arg(str(me))
    else:  # Assume production environment
        dt = getDate.query('iex_eod')
        yr = dt.year
        fpath = Path(baseDir().path, 'derivatives/end_of_day/temp', str(yr))
        globs = list(fpath.glob('**/*.parquet'))

        df_list = []
        [df_list.append(pd.read_parquet(path)) for path in globs]
        df_all = pd.concat(df_list)

    return df_all

# %% codecell


def get_cboe_ref():
    """Get cboe reference data for use on yfinance."""
    df = None
    path_suf = f"symref_{getDate.query('cboe')}.parquet"
    path = Path(baseDir().path, 'derivatives/cboe_symref', path_suf)
    if path.is_file():
        df = pd.read_parquet(path)
    else:
        url_p1 = 'https://www.cboe.com/us/options/market_statistics/'
        url_p2 = 'symbol_reference/?mkt=cone&listed=1&unit=1&closing=1'
        url = f"{url_p1}{url_p2}"
        get = requests.get(url)

        df = pd.read_csv(BytesIO(get.content), low_memory=False)
        df.to_parquet(path)

    cols_to_drop = ['Cboe Symbol', 'Closing Only']
    df = (df.rename(columns={'Underlying': 'symbol'})
            .drop(columns=cols_to_drop))

    return df

# %% codecell


def clean_yfinance_options(df_temp=False):
    """Align with cboe ref data. Clean. Convert columns."""
    df_comb = False
    path_suf = f"_{getDate.query('cboe')}.parquet"
    path = Path(baseDir().path, 'derivatives/end_of_day/daily_dump', path_suf)

    if path.is_file():
        df_comb = pd.read_parquet(path)
    else:
        if not df_temp:
            df_temp = return_yoptions_temp_all()

        cboe_ref = get_cboe_ref()
        cboe_ref['contractSymbol'] = cboe_ref['OSI Symbol'].str.replace(' ', '')
        df_comb = pd.merge(df_temp, cboe_ref, on=['contractSymbol']).copy()

        # Add column for puts and calls
        df_comb['side'] = df_comb['OSI Symbol'].str[-9]
        # Add expiration dates
        df_comb['expDate'] = df_comb['OSI Symbol'].str[-16:-9].str.replace(' ', '')
        df_comb['expDate'] = pd.to_datetime(df_comb['expDate'], format='%y%m%d')

        df_comb['date'] = pd.to_datetime(df_comb['date'], unit='ms')
        df_comb['lastTradeDate'] = pd.to_datetime(df_comb['lastTradeDate'], unit='ms')

        df_comb = dataTypes(df_comb, parquet=True).df

        df_comb.to_parquet(path)

    return df_comb

# %% codecell
