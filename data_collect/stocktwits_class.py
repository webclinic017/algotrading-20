"""
Let's have some fun with stocktwits data
- Rate limit of 200 get requests/hour with unauthenticated
- Rate limit of 400 get requests/hour with authenticated
- Unauthenticated is tied to IP address (rotating proxies)
"""
# %% codecell
#############################################################
import json
import os
import sys
from io import StringIO
import os.path
from pathlib import Path
import importlib
import datetime
from datetime import date, timedelta, time
from dotenv import load_dotenv
import gzip

from nested_lookup import nested_lookup
import requests
import pandas as pd


try:
    from scripts.dev.multiuse.help_class import dataTypes, baseDir
except ModuleNotFoundError:
    from multiuse.help_class import dataTypes, baseDir

# %% codecell
#############################################################

class stTrending():
    """Stock twits trending data."""
    st_base_url = "https://api.stocktwits.com/api/2/"
    st_base_path = f"{baseDir().path}/stocktwits/trending"

    def __init__(self):
        self.rec_df = self.request_data(self)
        self.format_data(self)
        self.check_for_exist(self)
        self.df = self.concat_and_write(self)


    @classmethod
    def request_data(cls, self):
        """Request data from stocktwits api."""
        # Trending stocks
        trend_u1 = "trending/symbols/equities.json"
        trend_url = f"{self.st_base_url}{trend_u1}"
        payload = {'limit': 29}

        stwits_get = requests.get(trend_url, params=payload)
        stwits = json.load(StringIO(stwits_get.content.decode('utf-8')))
        stwits_df = pd.DataFrame(stwits['symbols'])
        return stwits_df

    @classmethod
    def format_data(cls, self):
        """Format stocktwits data."""
        self.rec_df.drop(columns=['is_following', 'title', 'aliases'], inplace=True)
        self.rec_df = dataTypes(self.rec_df).df
        self.rec_df['timestamp'] = pd.Timestamp.now('US/Eastern')

    @classmethod
    def check_for_exist(cls, self):
        """Check existing dataframe."""
        # Construct file path
        self.st_fname = f"{self.st_base_path}/{date.today()}.gz"

        if os.path.isfile(self.st_fname):
            self.old_df = pd.read_json(self.st_fname, compression='gzip')
        else:
            self.old_df = pd.DataFrame()

    @classmethod
    def concat_and_write(cls, self):
        """Concat dataframes and write to json."""
        new_df = pd.concat([self.old_df, self.rec_df])
        new_df.reset_index(inplace=True, drop=True)
        # Write data to local json file
        new_df.to_json(self.st_fname, compression='gzip')

        return new_df

# %% codecell
#############################################################


class stwitsUserStream():
    """Get user messages and extract symbols/watch counts."""

    base_dir = f"{baseDir().path}/stocktwits/user_stream"
    base_url = "https://api.stocktwits.com/api/2/streams"

    def __init__(self):
        self.this_year = date.today().year
        user_list = self.make_user_list(self)
        self.get_messages(self, user_list)

    @classmethod
    def make_user_list(cls, self):
        """Make and return user list."""
        user_list = (['Nightmare919', 'Law306', 'billythekid123',
                      'TheInvestor23', 'InsiderFinance', 'Sherlock'])
        return user_list

    @classmethod
    def get_messages(cls, self, user_list):
        """Get user messages."""
        for u in user_list:
            user_fpath = f"{self.base_dir}/{u}"
            # Define local fpaths for raw and cleaned messages
            self.prev_fpath = f"{user_fpath}/raw_{self.this_year}.gz"
            self.syms_fpath = f"{user_fpath}/syms_{self.this_year}.gz"

            # Create local directory if it does not exist
            if not os.path.isdir(user_fpath):
                os.mkdir(user_fpath)
            user_url = f"{self.base_url}/user/{u}.json"
            # Data decoded
            self._get_ops(self, user_url, user_fpath)

    @classmethod
    def _get_ops(cls, self, user_url, user_fpath):
        """Data cleaning/getting ops."""
        st_get = requests.get(user_url)
        st_decode = json.load(StringIO(st_get.content.decode('utf-8')))

        [messages] = nested_lookup('messages', st_decode)
        raw_df = pd.DataFrame(messages)

        syms_df = self._clean_syms(self, st_decode, messages)

        if os.path.isfile(self.prev_fpath):
            raw_df = self._concat_drop(self.prev_fpath, raw_df)
        if os.path.isfile(self.syms_fpath):
            syms_df = self._concat_drop(self.syms_fpath, syms_df)

        self._write_to_json(self.prev_fpath, raw_df)
        self._write_to_json(self.syms_fpath, syms_df)

    @classmethod
    def _clean_syms(cls, self, st_decode, messages):
        """Extract symbols, watch_count, created_date."""
        msg_list = []
        idx = ['symbol', 'watchlist_count', 'created_at']
        for x in range(len(messages)):
            try:
                for y in range(len(messages[x]['symbols'])):
                    pre = messages[x]['symbols'][y]
                    row = (pre[idx[0]], pre[idx[1]], messages[x][idx[2]])
                    msg_list.append(row)
            except KeyError:  # If the message doesn't have any symbols
                pass

        col_list = ['symbol', 'watch_count', 'created_at']
        sym_df = pd.DataFrame(msg_list, columns=col_list)

        return sym_df

    @classmethod
    def _concat_drop(cls, fpath, st_df):
        """If prev file exists, concat, drop duplicates."""
        prev_df = pd.DataFrame()
        try:
            prev_df = pd.read_json(fpath, compression='gzip')
        except ValueError:
            pass

        comb_df = pd.concat([prev_df, st_df])
        # If the raw df, use ID for dropping duplicates
        if 'id' in comb_df.columns:
            comb_df.drop_duplicates(
                        subset=['id'],
                        inplace=True)
        else:  # Use symbol and created_at for dropping duplicates
            comb_df.drop_duplicates(
                        subset=['symbol', 'created_at'],
                        inplace=True)
        df = comb_df.copy(deep=True)
        return df

    @classmethod
    def _write_to_json(cls, fpath, df):
        """Reset index and write dataframe to json."""
        df.reset_index(inplace=True, drop=True)
        df.to_json(fpath, compression='gzip')

# %% codecell
#############################################################


class stWatch():
    """Get my stocktwits watchlist."""

    # refresh = True/False
    # yes == get new data, no == read local data, if it exists

    def __init__(self, refresh=True):
        self.get_fpath(self)
        self.data = self.get_data(self, refresh)

    @classmethod
    def get_fpath(cls, self):
        """Construct local fpath."""
        self.fpath = f"{baseDir().path}/stocktwits/me/_watch_{date.today()}.gz"

    @classmethod
    def get_data(cls, self, refresh):
        """Read local data if it exists."""
        data = ''
        if os.path.isfile(self.fpath) and refresh:
            # with gzip.open(self.fpath, 'rb') as f:
            #     data = f.read()
            data = json.load(gzip.open(self.fpath))
        else:
            data = self.request_data(self)

        return data

    @classmethod
    def request_data(cls, self):
        """Request fresh watchlist data from stocktwits."""
        st_url, payload = self.construct_params()
        # Request data
        st_get = requests.get(st_url, params=payload).json()

        # Create a list of tuples with relevant data
        rel_l = []  # Relevant list
        for rec in st_get['watchlist']['symbols']:
            rel_l.append((rec['symbol'], rec['title'], rec['watchlist_count']))

        rel_df = pd.DataFrame(rel_l)
        rel_df.to_json(self.fpath, compression='gzip')

        data = json.load(gzip.open(self.fpath))
        # with gzip.open(self.fpath, 'wb') as f:
        #     f.write(bytearray(rel_l))

        return data

    @classmethod
    def construct_params(cls):
        """Construct params used for st request."""
        load_dotenv()
        st_base_url = "https://api.stocktwits.com/api/2/watchlists/show"
        st_url = f"{st_base_url}/{os.environ.get('st_watch_id')}.json"
        payload = {'access_token': os.environ.get("st_token")}

        return st_url, payload

# %% codecell
#############################################################

def st_watchlist_id():
    """Get watchlist id for authenticating user."""
    load_dotenv()
    watch_url = "https://api.stocktwits.com/api/2/watchlists.json"
    payload = {'access_token': os.environ.get("st_token")}
    stwits_get = requests.get(watch_url, params=payload).json()

    return stwits_get['watchlists'][0]['id']

# %% codecell
#############################################################
