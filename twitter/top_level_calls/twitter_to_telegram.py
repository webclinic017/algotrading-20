"""Flow process from Twitter to Telegram."""
# %% codecell

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


def get_most_recent_messages_per_user(telegram=False, verbose=False, testing=False):
    """Get messages within the last 30 seconds."""
    # telegram keyword arg will then start the process of making a
    # user poll that sends to telegram
    th = TwitterHelpers.twitter_fpaths
    df_uref = pd.read_parquet(th('user_ref'))

    if verbose:
        call_list = []

    for index, row in df_uref.iterrows():
        kwargs = ({'username': row['username'],
                   'params': {'max_results': 5,  # smallest val is 5
                              'exclude': 'retweets,replies'}})
        # First call gets the first round of results - includes pag token
        call = TwitterAPI(method='user_tweets', **kwargs)
        # Should return df_view - most likely to be empty
        df_view = call.df

        if isinstance(df_view, pd.DataFrame):
            GetTimestampsForEachRelTweet(row['id'])
            if not df_view.empty and telegram:  # See if already recorded
                check_for_unsent_telegram_messages(row['username'], **kwargs)

        if verbose:
            call_list.append(call)

    if verbose:
        return call_list

# %% codcell


def check_for_unsent_telegram_messages(user_id=False, username=False, **kwargs):
    """Check if a message needs to be sent in telegram."""
    if username and not user_id:
        user_id = TwitterHelpers.twitter_lookup_id(username)

    fpath_ref = TwitterHelpers.twitter_fpaths('tweet_by_id', user_id)
    df_reft = pd.read_parquet(fpath_ref)
    # If dataframe is empty, then exit program and return False
    if df_reft.empty:
        if kwargs['verbose']:
            help_print_arg(f"df_uref empty for fpath {str(fpath_ref)}")
        return False

    if 'telegram_sent' not in df_reft.columns:
        df_reft['telegram_sent'] = 0  # 1 if sent

    # Only messages that haven't been sent
    cond_sent = (df_reft['telegram_sent'] == 0)
    # Only tweets in the last 60 seconds
    dt, secs = getDate.tz_aware_dt_now(), 60
    if kwargs['testing']:
        if 'time_filter' in kwargs.keys():
            secs = kwargs['time_filter']
        else:
            secs = 60**3

    cond_dt = (df_reft['created_at'].dt.to_pydatetime() >
               (dt - timedelta(seconds=secs)))

    rows = df_reft[cond_sent & cond_dt]

    if rows.empty:
        return False
    else:
        tids = rows['id'].tolist()
        df_all = make_tradeable_messages(user_id, tids)
        if not df_all.empty:
            send_telegram_trade_record_msg_sent(user_id, df_all, **kwargs)

        if kwargs['testing']:
            return {'rows': rows, 'trade_messages': df_all}

# %% codecell


def make_tradeable_messages(user_id, tids=None):
    """Make tradeable messages that can be sent to telegram chat."""
    # Ignore all this for the time being
    df = TwitterUserExtract(user_id).df_view

    cols_to_keep = (['entry', 'exit', 'sym_0', 'strike', 'side',
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

    if tids:  # If list of twitter ids
        df_all = df_all[df_all['id'].isin(tids)]

    return df_all

# %% codecell


def send_telegram_trade_record_msg_sent(user_id, df_msgs, **kwargs):
    """Send / test send messages for telegram."""
    user_dir = TwitterHelpers.twitter_fpaths('user_dir', user_id)
    fpath_tgrams = user_dir.joinpath('_telegram_msgs.parquet')

    fpath_reft = TwitterHelpers.twitter_fpaths('tweet_by_id', user_id)
    df_reft = pd.read_parquet(fpath_reft).reset_index(drop=True)

    if 'telegram_sent' not in df_reft.columns:
        df_reft['telegram_sent'] = 0

    for index, row in df_msgs.iterrows():
        # result = telegram_push_message(text=row['message'])
        result = telegram_push_poll(tid=row['id'], text=row['message'], **kwargs)
        if result:
            df_reft.loc[index, 'telegram_sent'] = 1

            df_tgrams = pd.json_normalize(result.json()['result'])
            df_tgrams['twitter_author_id'] = row['author_id']
            df_tgrams['twitter_tweet_id'] = row['id']
            df_tgrams.drop(columns='entities', inplace=True, errors='ignore')
            df_tgrams['date'] = pd.to_datetime(df_tgrams['date'], unit='s')

            write_to_parquet(df_tgrams, fpath_tgrams, combine=True)

        if kwargs['testing']:
            return result

    write_to_parquet(df_reft, fpath_reft)


# %% codecell
