"""Helper functions for handling yoptions/yfinance data."""
# %% codecell
from pathlib import Path
from io import BytesIO
from tqdm import tqdm
import requests
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, dataTypes, help_print_arg

# %% codecell


def yoptions_combine_last(all=False):
    """Combine all options with max date."""
    # Default is last. Change all to True for all_combined
    fpath = Path(baseDir().path, 'derivatives/end_of_day/2021')
    globs = list(fpath.glob('**/*.parquet'))

    df_list = []
    [df_list.append(pd.read_parquet(path)) for path in globs]
    df_all = pd.concat(df_list)

    path_suf = f"_{getDate.query('cboe')}.parquet"

    # Comine last aka today's data to combined folder
    if not all:
        df_today = df_all[df_all['date'] == df_all['date'].max()].copy()
        df_today.drop_duplicates(subset=['contractSymbol'], inplace=True)

        path = Path(baseDir().path, 'derivatives/end_of_day/combined', path_suf)

        df_today = dataTypes(df_today, parquet=True).df
        df_today.to_parquet(path)
    elif all:  # Combine all data to combined_all directory
        df_all.drop_duplicates(subset=['contractSymbol', 'date'], inplace=True)
        path = Path(baseDir().path, 'derivatives/end_of_day/combined_all', path_suf)
        df_all = dataTypes(df_all, parquet=True).df
        df_all.to_parquet(path)


# %% codecell


def yoptions_drop_hist_dupes():
    """Cycle through yoptions hist and drop duplicates."""
    dt = getDate.query('cboe')
    yr = dt.year
    path = Path(baseDir().path, 'derivatives/end_of_day/', str(yr))
    fpaths = list(path.glob('**/*.parquet'))

    for fpath in tqdm(fpaths):
        try:
            df = pd.read_parquet(fpath)
            df.drop_duplicates(subset=['contractSymbol', 'date'], inplace=True)
            dataTypes(df, parquet=True).df.to_parquet(fpath)
        except Exception as e:
            help_print_arg(e)

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


def get_cboe_ref(ymaster=False):
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

    # cols_to_drop = ['Cboe Symbol', 'Closing Only']
    df = df.rename(columns={'Underlying': 'symbol'})
    # .drop(columns=cols_to_drop))

    if ymaster:
        df = pd.DataFrame(df['symbol'].unique(), columns=['symbol']).copy()

    return df

# %% codecell


def clean_yfinance_options(df_temp=False, refresh=False):
    """Align with cboe ref data. Clean. Convert columns."""
    df_comb = False
    path_suf = f"_{getDate.query('cboe')}.parquet"
    path = Path(baseDir().path, 'derivatives/end_of_day/daily_dump', path_suf)

    if path.is_file() and not refresh:
        df_comb = pd.read_parquet(path)
        return df_comb
    else:
        if not isinstance(df_temp, pd.DataFrame):
            df_temp = return_yoptions_temp_all()

        cboe_ref = get_cboe_ref()
        cboe_ref['contractSymbol'] = cboe_ref['OSI Symbol'].str.replace(' ', '')
        df_comb = pd.merge(df_temp, cboe_ref, on=['contractSymbol']).copy()

        df_comb.drop_duplicates(subset=['contractSymbol'], inplace=True)
        # Add column for puts and calls
        df_comb['side'] = df_comb['OSI Symbol'].str[-9]
        # Add expiration dates
        df_comb['expDate'] = df_comb['OSI Symbol'].str[-16:-9].str.replace(' ', '')
        df_comb['expDate'] = pd.to_datetime(df_comb['expDate'], format='%y%m%d')

        df_comb['date'] = pd.to_datetime(df_comb['date'], unit='ms')
        df_comb['lastTradeDate'] = pd.to_datetime(df_comb['lastTradeDate'], unit='ms')

        df_comb['openInterest'] = df_comb['openInterest'].where(df_comb['openInterest'] != 0, 1)
        df_comb['vol/oi'] = df_comb['volume'].div(df_comb['openInterest']).round(0)
        df_comb['mid'] = (df_comb['ask'].add(df_comb['bid'])).div(2).round(3)
        df_comb['bid'] = df_comb['bid'].round(3)
        df_comb['premium'] = (df_comb['mid'].mul(df_comb['volume']) * 100).round(0)

        if 'strike_x' in df_comb.columns:
            df_comb['strike'] = df_comb['strike_x']

        df_comb = dataTypes(df_comb, parquet=True).df

        df_comb.to_parquet(path)

    return df_comb

# %% codecell


def get_yoptions_unfin():
    """Get all unfinished yoptions."""
    fpath = Path(baseDir().path, 'derivatives/end_of_day/unfinished')
    globs = list(fpath.glob('*.parquet'))

    df_list = []
    [df_list.append(pd.read_parquet(path)) for path in globs]
    df_all = pd.concat(df_list)
    df_syms = pd.DataFrame(df_all['symbol'].unique(), columns=['symbol'])

    return df_syms


def yoptions_still_needed(recreate=False):
    """Return a list of all syms:exp_dates that are missing."""
    ref_path = Path(baseDir().path, 'ref_data', 'syms_with_options.parquet')
    ref_df = pd.read_parquet(ref_path)


    path_for_temp = Path(baseDir().path, 'derivatives/end_of_day/temp/2021')
    paths_for_temp = list(path_for_temp.glob('**/*.parquet'))

    df_list = []
    for fpath in paths_for_temp:
        df_list.append(pd.read_parquet(fpath))

    df_all = pd.concat(df_list)
    df_collected = (df_all.groupby(by=['symbol'])['expDate']
                          .agg({lambda x: list(x)})
                          .reset_index()
                          .rename(columns={'<lambda>': 'expDatesStored'})
                          .copy())

    df_comb = pd.merge(ref_df, df_collected, how='left', on=['symbol'], indicator=True)
    df_left = df_comb[df_comb['_merge'] == 'left_only'].copy()

    # df_comb['expDatesNeeded'] = df_comb.apply(lambda row: list(set(row.expDates) - set(row.expDatesStored)), axis=1)

    # if recreate:
    #    df_comb = df_comb.drop(columns=['expDates', 'expDatesStored'])

    return df_left


# %% codecell
