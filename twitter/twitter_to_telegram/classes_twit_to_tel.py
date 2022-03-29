"""Twitter to telegram with classes."""
# %% codecell
import logging
import inspect
from datetime import timedelta
import time


import pandas as pd

try:
    from scripts.dev.twitter.top_level_calls.tweet_timestamps import GetTimestampsForEachRelTweet
    from scripts.dev.twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from scripts.dev.twitter.methods.helpers import TwitterHelpers
    from scripts.dev.twitter.twitter_api import TwitterAPI
    from scripts.dev.telegram.methods.poll import telegram_push_poll
    from scripts.dev.multiuse.help_class import getDate, help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from twitter.top_level_calls.tweet_timestamps import GetTimestampsForEachRelTweet
    from twitter.user_tweets.part2_clean_extract import TwitterUserExtract
    from twitter.methods.helpers import TwitterHelpers
    from twitter.twitter_api import TwitterAPI
    from telegram.methods.poll import telegram_push_poll
    from multiuse.help_class import getDate, help_print_arg, write_to_parquet


logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

# %% codecell


class TwitterRecentMessagesUser():
    """Recent messages per user, get timestamps if necessary."""
    # trmu = TwitterRecentMessagesUser

    def __init__(self, **kwargs):
        self._trmu_initiate_loop(self, **kwargs)

    @classmethod
    def _trmu_initiate_loop(cls, self, **kwargs):
        """Initiate loop to pull most recent tweets for each user."""
        for index, row in self.df_uref.iterrows():
            udict = self.user_dict[row['id']]
            try:
                df = self._trmu_get_return_tweets(self, index, row)
                if isinstance(df, pd.DataFrame):
                    if not df.empty:  # id is user_id, from df_uref
                        gte_df = GetTimestampsForEachRelTweet(row['id'], testing=False, verbose=self.verbose).df
                        # Print # of timestamps needed
                        if self.verbose:
                            msg = f"There were {str(gte_df.shape[0])} timestamps needed"
                            help_print_arg(msg, isp=inspect.stack())
                    # Refresh tweet_by_id df if new timestamps were added
                    self.user_dict[row['id']]['tweet_by_id'] = pd.read_parquet(udict['fpath_reft'])
            except Exception as e:
                uname = udict['username']
                help_print_arg('')
                help_print_arg(f'TwitterRecentMessagesUser failed for {uname}')
                self.elog(self, e)
                continue

    @classmethod
    def _trmu_get_return_tweets(cls, self, index, row):
        """Get tweets, timestamps."""
        start_time = getDate.tz_aware_dt_now(offset=self.offset, rfcc=True, utc=True)
        end_time = getDate.tz_aware_dt_now(rfcc=True, utc=True)

        if self.verbose:
            help_print_arg(f"Start time: {start_time}. End time: {end_time}")

        params = ({'username': row['username'],
                   'params': {'max_results': self.max_results,
                              # smallest val is 5 ^
                              'start_time': start_time,
                              'end_time': end_time,
                              'exclude': 'retweets,replies'}})
        # First call gets the first round of results - includes pag token
        call = TwitterAPI(method='user_tweets', **params)
        self.call_list.append(call)
        time.sleep(.5)
        # Returns a twitter API object - not df_view
        df = call.df.copy()

        if self.verbose:
            crh = call.get.raw.getheaders()
            msg = f"{row['username']} Twitter calls remaining {crh['x-rate-limit-remaining']}"
            help_print_arg('')
            help_print_arg(msg)

        return df


class TwitterUnsentTelegramMsgs():
    """TwitterToTelegram -> unsent telegram messages."""
    # tutm = TwitterUnsentTelegramMsgs

    def __init__(self, **kwargs):
        self._tutm_initiate_loop(self, **kwargs)

    @classmethod
    def _tutm_initiate_loop(cls, self, **kwargs):
        """Initiate loop to pull most recent tweets for each user."""
        for index, row in self.df_uref.iterrows():
            udict = self.user_dict[row['id']]
            uname = udict['username']
            try:
                self._tutm_check_tweet_ref_trades(self, row['id'])
                if self.verbose:
                    help_print_arg('')
                    help_print_arg(f"{uname}: TwitterUnsentTelegramMsgs")
            except Exception as e:
                uname = udict['username']
                help_print_arg('')
                help_print_arg(f'TwitterUnsentTelegramMsgs failed for {uname}')
                self.elog(self, e)
                continue

    @classmethod
    def _tutm_check_tweet_ref_trades(cls, self, user_id, **kwargs):
        """Check user tweet_ref for new possible trade msgs."""
        df_reft = self.user_dict[user_id]['tweet_by_id']
        if df_reft.empty:  # Print out fpath
            if self.verbose:
                help_print_arg(f"df_uref empty for fpath {str(df_reft)}")
            return False

        # Add column, fill nas if they exist
        df_reft['telegram_sent'] = df_reft.get('telegram_sent', pd.Series(0))
        df_reft['telegram_sent'] = df_reft['telegram_sent'].fillna(0)
        # Only tweets from today
        dt, secs = getDate.tz_aware_dt_now(), 75
        # These hardly work due to the lack of consistent datetime schema
        # Replace with if these are the same day, rather than within 75 seconds
        cond_dt = (df_reft['created_at'].dt.date == dt.date())
        if self.testing:
            test_dt = (dt.date() - timedelta(days=7))
            cond_dt = (df_reft['created_at'].dt.date >= test_dt)
            # Not used
            # secs = self.time_filter

        # Only messages that haven't been sent
        cond_sent = (df_reft['telegram_sent'] == 0)

        rows = df_reft[cond_dt & cond_sent]
        if self.send_anyway:  # For testing purposes, resend sent messages
            rows = df_reft[cond_dt]
        rows_na = rows.dropna(subset='tcode')

        if not rows.empty and rows_na.empty:
            help_print_arg("No valid tcodes in rows")

        self.user_dict[user_id]['rows'] = rows
        self.user_dict[user_id]['tids'] = rows['id'].tolist()
        if rows.empty:
            if self.verbose:
                msg = "Rows empty"
                isp = inspect.stack()
                help_print_arg(msg, isp=isp)


class MakeTradeableMessages():
    """Make df_msgs (tradeable messages) to send to telegram."""
    # mtm = MakeTradeableMessages

    def __init__(self, **kwargs):
        self._mtm_initiate_loop(self, **kwargs)

    @classmethod
    def _mtm_initiate_loop(cls, self, **kwargs):
        """Initiate loop to pull most recent tweets for each user."""
        for index, row in self.df_uref.iterrows():
            udict = self.user_dict[row['id']]
            uname = udict['username']
            try:
                if self.verbose:
                    help_print_arg('')
                    help_print_arg(f"{uname}: MakeTradeableMessages")

                if not udict['rows'].empty:
                    self._mtm_get_mod_df_rows(self, row['id'], **kwargs)
                elif self.verbose:
                    uname = udict['username']
                    msg = "MakeTradeableMessages: skipping loop: self.rows empty"
                    help_print_arg(f"{uname} {msg}")
                else:
                    pass
            except Exception as e:
                uname = udict['username']
                help_print_arg('')
                help_print_arg(f'MakeTradeableMessages failed for {uname}')
                self.elog(self, e)
                continue

    @classmethod
    def _mtm_get_mod_df_rows(cls, self, user_id, **kwargs):
        """Get df_rows from TwitterUnsentTelegramMsgs."""
        tids = self.user_dict[user_id]['tids']
        df = self.user_dict[user_id]['rows']

        df = TwitterHelpers.parse_tcode(df)
        cols = df.columns
        if 'entry' not in cols and 'exit' not in cols:
            df_view = TwitterUserExtract(user_id).df_view[['id', 'entry', 'exit']]
            df = df.merge(df_view, on='id', how='left')

        cols_to_keep = (['entry', 'exit', 'symbol', 'strike', 'side', 'tcode',
                         'text', 'id', 'author_id', 'created_at'])
        df_t1 = df[cols_to_keep].copy()

        s_to_replace = (df_t1[df_t1['side'].apply(lambda row: row in ['p', 'c'])]
                        ['side']
                        .str.replace('p', 'put', regex=False)
                        .str.replace('c', 'call', regex=False))

        df_t1.loc[s_to_replace.index, 'side'] = s_to_replace
        df_t1['entry'] = df_t1['entry'].fillna(False)
        df_t1['exit'] = df_t1['exit'].fillna(False)
        self.user_dict[user_id]['df_msgs_mod1'] = df_t1

        df_msgs = self._mtm_df_buy_sell_all(self, df_t1, tids, **kwargs)
        self.user_dict[user_id]['df_msgs'] = df_msgs
        if self.verbose:
            # help_print_arg(f"make_tradeable_messages df_msgs: {str(df_msgs.head(1))}")
            help_print_arg(f"make_tradeable_messages df_msgs: {str(df_msgs.shape[0])}")

    @classmethod
    def _mtm_df_buy_sell_all(cls, self, df, tids, **kwargs):
        """Entry, exit, and return formatted df with trade messages."""
        # Reduce to trade "entries"
        df_buys = df[df['entry']].copy()
        if self.verbose:
            help_print_arg(f"make_tradeable_messages df_msgs: {str(df_buys.shape[0])}")
        if not df_buys.empty:
            df_buys['message'] = (df_buys.apply(lambda row: f"Buy {row['symbol']} "
                                   f"${str(row['strike'])} {row['side']}: OG Tweet: "
                                                  f"{row['text']}", axis=1))
        df_sells = df[df['exit']].copy()
        if not df_sells.empty:
            df_sells['message'] = (df_sells.apply(lambda row: f"Sell {row['symbol']} "
                                   f"${str(row['strike'])} {row['side']}: OG Tweet: "
                                                  f"{row['text']}", axis=1))

        df_all = pd.concat([df_buys, df_sells])

        if tids:  # If list of twitter ids, keep only rows with recent tids
            df_all = df_all[df_all['id'].isin(tids)]
            if self.verbose:
                help_print_arg("""MakeTradeableMessages._df_buy_sell_all :
                               user tids (twitter ids) applied""")

        return df_all


class SendTelegramPollFromTrade():
    """Send poll to telegram with trade info, record message sent."""
    # stpft = SendTelegramPollFromTrade

    def __init__(self, **kwargs):
        self._stpft_initiate_loop(self, **kwargs)

    @classmethod
    def _stpft_initiate_loop(cls, self, **kwargs):
        """Initiate loop to pull most recent tweets for each user."""
        for index, row in self.df_uref.iterrows():
            udict = self.user_dict[row['id']]
            uname = udict['username']
            try:
                if self.verbose:
                    help_print_arg('')
                    help_print_arg(f"{uname}: SendTelegramPollFromTrade")

                if not udict['df_msgs'].empty:
                    self._stpft_get_df_trades(self, row['id'], **kwargs)
                else:
                    uname = udict['username']
                    help_print_arg(f"""{uname} SendTelegramPollFromTrade:
                                   skipping loop: df_msgs empty""")
            except Exception as e:
                uname = udict['username']
                help_print_arg('')
                help_print_arg(f'SendTelegramPollFromTrade failed for {uname}')
                self.elog(self, e)
                continue

    @classmethod
    def _stpft_get_df_trades(cls, self, user_id, **kwargs):
        """Get df_trades before telegram message send iteration."""
        if self.verbose:
            help_print_arg('')
            help_print_arg(f"send_telegram_trade_record_msg_sent: {str(kwargs)}")

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
        df_reft['telegram_sent'] = (df_reft.get('telegram_sent', pd.Series(0))
                                           .fillna(0))
        # Load all trades
        tcode_idx = []
        df_trades = TwitterHelpers.tf('user_trades', user_id, return_df=True)
        if isinstance(df_trades, pd.DataFrame):
            tcode_idx = df_trades.index

        # df_trades before message send / modifications
        self.user_dict[user_id]['df_trades_pre'] = df_trades
        self.user_dict[user_id]['tgram_msgs'] = tgram_msgs
        self.user_dict[user_id]['tcode_idx'] = tcode_idx
        self.user_dict[user_id]['tgram_results'] = []

        self._stpft_send_poll_rcd_msg(self, user_id, **kwargs)

    @classmethod
    def _stpft_send_poll_rcd_msg(cls, self, user_id, **kwargs):
        """Send poll and record meta data, modify df_trades."""
        udict = self.user_dict[user_id]
        df_msgs = udict['df_msgs']
        tgram_msgs = udict['tgram_msgs']
        tcode_idx = udict['tcode_idx']
        df_reft = udict['tweet_by_id']

        # Iterate through messages to be sent
        for index, row in df_msgs.iterrows():
            # twitter_tweet_id = row['id']
            if row['id'] in tgram_msgs:
                if self.verbose:
                    msg1 = "send_telegram_trade_record_msg_sent"
                    help_print_arg(f"{msg1}: id {str(row['id'])} already sent")
                # If self.send_anyway - resend message to testing channel
                if not self.send_anyway:
                    continue  # Check if this message has already been sent
            elif row['tcode'] not in tcode_idx and row['exit']:
                if self.verbose:
                    help_print_arg(f"""send_telegram_trade_record_msg_sent
                                   {msg1}: no entry signal for
                                   {str(row['id'])}""")
                continue  # Check if this is a sell msg, but buy was missed

            result = telegram_push_poll(tid=row['id'], text=row['message'], **kwargs)
            self.user_dict[user_id]['telegram_result'] = result
            if result:
                # Set the "df trade" position to 1, meaning sent
                df_reft.at[index, 'telegram_sent'] = 1

                # Parse json data, convert to df
                df_tgrams = pd.json_normalize(result.json()['result'])
                df_tgrams['twitter_author_id'] = row['author_id']
                df_tgrams['twitter_tweet_id'] = row['id']
                df_tgrams.drop(columns='entities', inplace=True, errors='ignore')
                df_tgrams['date'] = pd.to_datetime(df_tgrams['date'], unit='s')
                # Combine with local file, if it exists
                if not self.testing:
                    write_to_parquet(df_reft, udict['fpath_reft'], combine=True)
                    write_to_parquet(df_tgrams, udict['fpath_tgrams'], combine=True)
                if self.testing:
                    udict['tgram_results'].append(result)
