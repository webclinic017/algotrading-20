"""Flow process from Twitter to Telegram."""
# %% codecell
import time
import inspect
from inspect import signature
from datetime import timedelta

import pandas as pd

try:
    from scripts.dev.twitter.twitter_api import TwitterAPI
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.twitter.top_level_calls.tweet_timestamps import GetTimestampsForEachRelTweet
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.telegram.methods.poll import telegram_push_poll
    from scripts.dev.multiuse.help_class import getDate, write_to_parquet, help_print_arg
except ModuleNotFoundError:
    from twitter.twitter_api import TwitterAPI
    from twitter.methods.helpers import TwitterHelpers
    from twitter.top_level_calls.tweet_timestamps import GetTimestampsForEachRelTweet
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from telegram.methods.poll import telegram_push_poll
    from multiuse.help_class import getDate, write_to_parquet, help_print_arg


# %% codecell


def get_most_recent_messages_per_user(telegram=False, **kwargs):
    """Get messages within the last 30 seconds."""
    isp = inspect.stack()
    # telegram keyword arg will then start the process of making a
    # user poll that sends to telegram
    th = TwitterHelpers.twitter_fpaths
    df_uref = pd.read_parquet(th('user_ref'))

    testing = kwargs.get('testing', False)
    verbose = kwargs.get('verbose', False)
    max_results = kwargs.get('max_results', 5)
    offset = kwargs.get('offset', 75)

    if verbose:
        call_list = []

    for index, row in df_uref.iterrows():
        start_time = getDate.tz_aware_dt_now(offset=offset, rfcc=True, utc=True)
        end_time = getDate.tz_aware_dt_now(rfcc=True, utc=True)

        params = ({'username': row['username'],
                   'params': {'max_results': max_results,  # smallest val is 5
                              'start_time': start_time,
                              'end_time': end_time,
                              'exclude': 'retweets,replies'}})
        # First call gets the first round of results - includes pag token
        call = TwitterAPI(method='user_tweets', **params)
        time.sleep(.5)
        # Returns a twitter API object - not df_view
        df_view = call.df

        if verbose:
            crh = call.get.raw.getheaders()
            msg = f"{row['username']} Twitter calls remaining {crh['x-rate-limit-remaining']}"
            help_print_arg(msg)
            call_list.append(call)

        if isinstance(df_view, pd.DataFrame):
            if not df_view.empty:  # id is user_id, from df_uref
                gte_df = GetTimestampsForEachRelTweet(row['id'], testing=False).df
                if verbose:
                    msg = f"there are {str(gte_df.shape[0])} timestamps needed"
                    help_print_arg(msg, isp)
                if telegram:  # See if already recorded - default no
                    check_for_unsent_telegram_messages(user_id=row['id'], verbose=True, testing=True)

    if testing:
        return call_list

# %% codecell


def check_for_unsent_telegram_messages(user_id=False, **kwargs):
    """Check if a message needs to be sent in telegram."""
    verbose = kwargs.get('verbose', False)
    testing = kwargs.get('testing', False)
    isp = inspect.stack()
    # if username and not user_id:
    #    user_id = TwitterHelpers.twitter_lookup_id(username)

    fpath_ref = TwitterHelpers.twitter_fpaths('tweet_by_id', user_id)
    df_reft = pd.read_parquet(fpath_ref)
    # If dataframe is empty, then exit program and return False
    if df_reft.empty:
        if verbose:
            help_print_arg(f"df_uref empty for fpath {str(fpath_ref)}")
        return False

    if 'telegram_sent' not in df_reft.columns:
        df_reft['telegram_sent'] = 0  # 1 if sent

    # Only tweets in the last 60 seconds
    dt, secs = getDate.tz_aware_dt_now(), 75
    if testing:
        if 'time_filter' in kwargs.keys():
            secs = kwargs['time_filter']
        else:
            secs = 60**3

    cond_dt = (df_reft['created_at'].dt.to_pydatetime() >
               (dt - timedelta(seconds=secs)))
    # Only messages that haven't been sent
    cond_sent = (df_reft['telegram_sent'] == 0)

    rows = df_reft[cond_dt & cond_sent]

    if rows.empty:
        if verbose:
            msg = "Rows empty"
            help_print_arg(msg, isp=isp)
        return False
    else:
        # Convert all tweet ids to list
        tids = rows['id'].tolist()
        df_msgs = make_tradeable_messages(user_id, tids)
        if not df_msgs.empty:
            send_telegram_trade_record_msg_sent(user_id, df_msgs, **kwargs)

        if kwargs.get('testing', None):
            return {'rows': rows, 'trade_messages': df_msgs}

# %% codecell


def make_tradeable_messages(user_id, tids=None, df=None):
    """Make tradeable messages that can be sent to telegram chat."""
    if df is None:
        # Ignore all this for the time being
        df = TwitterUserExtract(user_id).df_view

    cols_to_keep = (['entry', 'exit', 'sym_0', 'strike', 'side', 'tcode',
                     'text', 'id', 'author_id', 'created_at'])
    df_t1 = df[cols_to_keep].copy()

    s_to_replace = (df_t1[df_t1['side'].apply(lambda row: row in ['p', 'c'])]
                    ['side']
                    .str.replace('p', 'put', regex=False)
                    .str.replace('c', 'call', regex=False))

    df_t1.loc[s_to_replace.index, 'side'] = s_to_replace

    df_buys = df_t1[df_t1['entry']].copy()
    df_buys['message'] = (df_buys.apply(lambda row: f"Buy {row['sym_0']} "
                          f"${str(row['strike'])} {row['side']}: OG Tweet: "
                                        f"{row['text']}", axis=1))

    df_sells = df_t1[df_t1['exit']].copy()
    df_sells['message'] = (df_sells.apply(lambda row: f"Sell {row['sym_0']} "
                           f"${str(row['strike'])} {row['side']}: OG Tweet: "
                                          f"{row['text']}", axis=1))

    df_all = pd.concat([df_buys, df_sells])

    if tids:  # If list of twitter ids, keep only rows with recent tids
        df_all = df_all[df_all['id'].isin(tids)]

    return df_all

# %% codecell


def send_telegram_trade_record_msg_sent(user_id, df_msgs, **kwargs):
    """Send / test send messages for telegram."""
    # All messages sent to Telegram
    user_dir = TwitterHelpers.tf('user_dir', user_id)
    fpath_tgrams = user_dir.joinpath('_telegram_msgs.parquet')
    tgram_msgs = []
    if fpath_tgrams.exists():
        df_old_tgrams = pd.read_parquet(fpath_tgrams)
        tgram_msgs = df_old_tgrams['twitter_tweet_id'].tolist()

    # All tweets that match "trade" tweets
    fpath_reft = TwitterHelpers.tf('tweet_by_id', user_id)
    df_reft = pd.read_parquet(fpath_reft).reset_index(drop=True)
    # If column not in the "trade df", add it and set value = 0
    if 'telegram_sent' not in df_reft.columns:
        df_reft['telegram_sent'] = 0
    # Load all trades
    df_trades = TwitterHelpers.tf('user_trades', user_id, return_df=True)
    if isinstance(df_trades, pd.DataFrame):
        tcode_idx = df_trades.index
    else:
        tcode_idx = []

    # Iterate through messages to be sent
    for index, row in df_msgs.iterrows():
        # twitter_tweet_id = row['id']
        if row['id'] in tgram_msgs:
            continue  # Check if this message has already been sent
        elif row['tcode'] not in tcode_idx and row['exit']:
            continue  # Check if this is a sell msg, but buy was missed

        result = telegram_push_poll(tid=row['id'], text=row['message'], **kwargs)
        if result:
            # Set the "df trade" position to 1, meaning sent
            df_reft.loc[index, 'telegram_sent'] = 1
            write_to_parquet(df_reft, fpath_reft, combine=True)

            # Parse json data, convert to df
            df_tgrams = pd.json_normalize(result.json()['result'])
            df_tgrams['twitter_author_id'] = row['author_id']
            df_tgrams['twitter_tweet_id'] = row['id']
            df_tgrams.drop(columns='entities', inplace=True, errors='ignore')
            df_tgrams['date'] = pd.to_datetime(df_tgrams['date'], unit='s')
            # Combine with local file, if it exists
            write_to_parquet(df_tgrams, fpath_tgrams, combine=True)

        if kwargs['testing']:
            return result

    write_to_parquet(df_reft, fpath_reft)


# %% codecell
