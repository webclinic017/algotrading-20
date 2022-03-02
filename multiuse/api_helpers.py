"""Helper functions for APIs."""
# %% codecell
#################################
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from pathlib import Path

import pandas as pd
import requests

try:
    from scripts.dev.multiuse.help_class import baseDir, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, help_print_arg, write_to_parquet

# %% codecell
#################################


class RecordAPICalls():
    """Record API calls."""

    def __init__(self, rep, name, **kwargs):
        self.fpath = self._get_fpath(self, name)
        self.df = self._base_parse_response(self, rep, name, **kwargs)
        self._write_to_file(self, self.df, self.fpath, name, **kwargs)

    @classmethod
    def _get_fpath(cls, self, name):
        """Get fpath from dict of log_name."""
        bpath = Path(baseDir().path, 'logs')
        fdict = ({
            'twitter': 'twitter/api_calls.parquet',
            'celery': 'celery/api_calls.parquet',
            'etrade': 'etrade/api_calls.parquet',
            'tdma': 'tdma/api_calls.parquet'
        })

        fpath = bpath.joinpath(fdict[name])
        return fpath

    @classmethod
    def _base_parse_response(cls, self, rep, name, **kwargs):
        """Construct df around response object - base method."""
        heads = rep.raw.getheaders()
        headers = {key: val for key, val in heads.items()}
        df = pd.Series(headers).to_frame().T
        df['name'] = name
        # Check if method passed
        if 'method' in kwargs.keys():
            df['method'] = kwargs['method']

        df['status_code'] = rep.status_code
        df['url'] = rep.url
        df['reason'] = rep.reason

        return df

    @classmethod
    def _write_to_file(cls, self, df, fpath, name, **kwargs):
        """Write df to local parquet file."""
        verbose = kwargs.get('verbose', None)
        if verbose:
            help_print_arg(f"RecordAPICalls: {name}: {str(fpath)}")

        write_to_parquet(df, fpath, combine=True)


# %% codecell


def get_sock5_nord_proxies(full_df=False):
    """Get a list of sock5 nord proxies."""
    nord_url = 'https://nordvpn.com/api/server'
    get = requests.get(nord_url)

    servers = pd.DataFrame(get.json())
    # Extract features keys for column names
    col_list = list(servers['features'].iloc[0].keys())
    servers[col_list] = servers.features.apply(lambda row: pd.Series(row))
    socks_df = servers[servers['socks']].copy()

    # Local local environment variables
    load_dotenv()
    nord_user = os.environ.get("nord_user")
    nord_pass = os.environ.get("nord_pass")

    # Construct the 1st part of the url
    purl = f"socks5://{nord_user}:{nord_pass}@"
    # Iterate over df to build proxy dict for each row
    socks_df['socksp'] = socks_df.apply(lambda row:
                                        {'http': f"{purl}{row.domain}:1080",
                                         'https': f"{purl}{row.domain}:1080"},
                                        axis=1)
    result = False
    if full_df:
        result = socks_df
    else:
        result = socks_df['socksp'].to_numpy()

    return result


def rate_limit(func_to_execute, master_limit=0, duration=60, counter_limit=199, testing=False, **kwargs):
    """Rate limit an API."""
    counter = 0
    master_count = 0
    finished, sym, sym_list, ytd = False, False, False, False
    if 'symbol' in kwargs.keys():
        sym = kwargs['symbol']
    elif 'sym' in kwargs.keys():
        sym = kwargs['sym']
    elif 'sym_list' in kwargs.keys():
        sym_list = kwargs['sym_list']
        master_limit = len(sym_list)

    if 'ytd' in kwargs.keys():
        ytd = kwargs['ytd']

    def sleep_if_hit(counter, next_min):
        """Program should sleep if rate limit is hit."""
        if (next_min <= datetime.now()) or (counter >= counter_limit):
            secs_to_sleep = (next_min - datetime.now()).seconds
            # print(f"Sleeping for {secs_to_sleep} seconds")
            time.sleep(secs_to_sleep)
            counter = 0
            next_min = datetime.now() + timedelta(seconds=duration)

        return counter, next_min

    if not finished:
        next_min = datetime.now() + timedelta(seconds=duration)
        if sym_list:
            for sym in sym_list:
                if testing:
                    help_print_arg(sym)
                counter += 1
                master_count += 1

                if ytd:
                    func_to_execute(sym, ytd=True)
                else:
                    func_to_execute(sym)

                counter, next_min = sleep_if_hit(counter, next_min)

                if master_count < master_limit:
                    continue
                else:
                    finished = True
                    break
        else:
            if finished:
                return
            else:
                if sym:
                    func_to_execute(sym)
                else:
                    func_to_execute()
                counter += 1
                master_count += 1
        # print(counter)
