"""Clean stocktwits user stream."""
# %% codecell

import pandas as pd

try:
    from scripts.dev.data_collect.stocktwits.helpers.paths import StockTwitsPaths
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.multiuse.help_class import write_to_parquet
except ModuleNotFoundError:
    from data_collect.stocktwits.helpers.paths import StockTwitsPaths
    from multiuse.class_methods import ClsHelp
    from multiuse.help_class import write_to_parquet

# %% codecell


class StockTwitsUserStream(ClsHelp, StockTwitsPaths):
    """V2 of the stocktwits user messages."""

    pop_list = (['symbols', 'user', 'entities',
                 'conversation', 'likes'])
    test_msg = None

    def __init__(self, get, **kwargs):
        StockTwitsPaths.__init__(self)
        self.testing = kwargs.get('testing', False)
        self.df_top = self._top_level_user_data(self, get, **kwargs)
        self.msg_list = self._extract_msg_list(self, **kwargs)
        self.dmsg_lists = self._unpack_msgs(self, self.msg_list, **kwargs)
        if isinstance(self.dmsg_lists, dict):
            self._concat_write(self, self.dmsg_lists)

    @classmethod
    def _top_level_user_data(cls, self, get, **kwargs):
        """Get the user level data + msg_list."""
        df_top = pd.json_normalize(get.json())
        return df_top

    @classmethod
    def _extract_msg_list(cls, self, **kwargs):
        """Extract message list and drop from df_top.columns."""
        msg_list = self.df_top['messages'].iloc[0]
        self.df_top.drop(columns='messages', inplace=True)
        return msg_list

    @classmethod
    def _unpack_msgs(cls, self, msg_list, **kwargs):
        """Unpack message list."""
        dmsg_lists = {'msgs': [], 'syms': []}

        for n, m in enumerate(msg_list):
            msg = msg_list[n]
            try:
                df_else, df_symbols = self._create_dfs(self, msg, **kwargs)
                df_msg = self._clean_msgs_create_df(self, msg, df_else, **kwargs)
                df_syms = self._df_syms(self, msg, df_symbols, df_msg, **kwargs)

                dmsg_lists['msgs'].append(df_msg)
                dmsg_lists['syms'].append(df_syms)
            except KeyError as ke:
                if self.testing:
                    self.elog(self, ke)
                    self.test_msg = msg
                    return
                else:
                    continue

        return dmsg_lists

    @classmethod
    def _create_dfs(cls, self, msg, **kwargs):
        """Iterate through message, extract contents."""
        if 'symbols' in msg.keys():
            df_symbols = (pd.json_normalize(msg.get('symbols', {}))
                            .rename(columns={'id': 'symbol.id'}, errors='ignore'))
        else:
            df_symbols = pd.DataFrame()

        df_else = (self.df_top.join(
                   df_symbols.join(pd.json_normalize(msg.get('conversation', {})))
                             .join(pd.json_normalize(msg.get('likes', {})))
                             .join(pd.json_normalize(msg.get('entities', {})))))
        return df_else, df_symbols

    @classmethod
    def _clean_msgs_create_df(cls, self, msg, df_else, **kwargs):
        """Clean messages."""
        pop_list = ['symbols', 'user', 'entities', 'conversation', 'likes']
        for p in pop_list:
            msg.pop(p, False)

        df_msg = pd.json_normalize(msg).rename(columns={'id': 'message.id'})
        df_msg = (df_msg.join(df_else)
                        .drop(columns=['user_ids'], errors='ignore').copy())
        return df_msg

    @classmethod
    def _df_syms(cls, self, msg, df_symbols, df_msg, **kwargs):
        """Create df of all symbols in message."""
        sym_cols = (['message.id', 'user.id', 'user.username', 'created_at'])
        df_symbols['user.id'] = df_msg['user.id'].iloc[0]
        df_syms = df_symbols.merge(df_msg[sym_cols], on='user.id', how='left')
        return df_syms

    @classmethod
    def _concat_write(cls, self, dmsg_lists):
        """Concat and write to local files."""
        kwargs = {'cols_to_drop': ['message.id'], 'combine': True}

        if dmsg_lists['msgs']:
            all_msgs = pd.concat(dmsg_lists['msgs'])
            write_to_parquet(all_msgs, self.f_msgs, **kwargs)
        if dmsg_lists['syms']:
            all_syms = pd.concat(dmsg_lists['syms'])
            write_to_parquet(all_syms, self.f_syms, **kwargs)
