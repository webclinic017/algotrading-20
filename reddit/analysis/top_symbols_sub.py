"""Top stock sybols mentioned for each subreddit."""
# %% codecell

import pandas as pd
from nltk.corpus import stopwords

try:
    from scripts.dev.multiuse.nlp_base import NlpHelpers
    from scripts.dev.data_collect.sec.sec_routines import sec_sym_list
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from multiuse.nlp_base import NlpHelpers
    from data_collect.sec.sec_routines import sec_sym_list
    from api import serverAPI

# %% codecell


class RedditTopSymbolsMentioned():
    """Get a count with optional date parameter for symbols mentioned."""

    # self.df_sub : dataframe for subreddit
    rtsm_dict = ({'analysis_top_symbols': 'rtsm'})

    def __init__(self, method, **kwargs):
        if method == 'analysis_top_symbols':
            self._rtsm_get_class_vars(self, **kwargs)
            self.df_sub = self._rtsm_get_data(self, **kwargs)
            self.reg_tickers = self._rtsm_get_regex_tickers(self, **kwargs)
            self.df_syms = self._rtsm_search_comment_body(self, self.df_sub)
            self._stopwords_other_exclude(self, **kwargs)
            self.df_syms_count = self._filter_out_syms_exclude_idx(self)

    @classmethod
    def _rtsm_get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)

        self.subreddit = kwargs.get('subreddit', 'wallstreetbets')
        self.dt = kwargs.get('dt', False)

    @classmethod
    def _rtsm_get_data(cls, self, **kwargs):
        """Get subreddit comment data from server."""
        df_sub = serverAPI('reddit_data', subreddit=self.subreddit).df
        df_sub['created_dt_utc'] = (pd.to_datetime(df_sub['created_utc'],
                                                   unit='s'))
        if self.dt:  # If optional datetime.date passed in kwargs
            df_sub = df_sub[df_sub['created_dt_utc'].dt.date == self.dt].copy()
        # Return subreddit dataframe
        return df_sub

    @classmethod
    def _rtsm_get_regex_tickers(cls, self, **kwargs):
        """Get sec (updated) ticker data from server."""
        df_sec = sec_sym_list(return_df=True)
        # Get regex of uppercase symbols
        tickers = df_sec['ticker'].tolist()
        reg_tickers = f" ?[^A-Z]\$?({'|'.join(tickers)})[^a-z]? "
        return reg_tickers

    @classmethod
    def _rtsm_search_comment_body(cls, self, df_sub):
        """Search comment body, return dataframe of extracted symbol counts."""
        df_matches = self.df_sub['body'].str.extractall(self.reg_tickers)
        df_report = (df_sub[df_sub['body']
                     .str.contains('User Report', regex=True)])
        df_matches = (df_matches[~df_matches.index.get_level_values(0)
                                                  .isin(df_report.index)])
        df_syms = df_matches[df_matches[0].str.len() > 1].copy()

        return df_syms

    @classmethod
    def _stopwords_other_exclude(cls, self, **kwargs):
        """Compile stopwords and exclude other items."""
        syms_to_remove = ['DD', 'DRS', 'IS', 'ME', 'RYAN', 'A', 'I', 'AN']
        words_drop = stopwords.words('english')
        words_drop_upper = [w.upper() for w in words_drop]
        syms_to_remove = syms_to_remove + words_drop_upper

        df_syms_drop = self.df_syms.reset_index(level=1, drop=True)
        syms_idx_dupe_all = df_syms_drop[df_syms_drop.index.duplicated()]
        idx_dupe_keep = (syms_idx_dupe_all
                         .reset_index()
                         .drop_duplicates(subset=['index', 0])['index']
                         .tolist())
        idx_to_keep = (df_syms_drop.index.difference(
                       syms_idx_dupe_all.index
                       ).append(pd.Index(idx_dupe_keep)))

        df_wsb1 = (NlpHelpers.get_all_uppercase_proportion(
                   self.df_sub[['body']], 'body'))
        mostly_caps_idx = df_wsb1[df_wsb1['upper%'] > .75].index
        idx_to_keep = (idx_to_keep.difference(mostly_caps_idx))

        self.syms_to_remove = syms_to_remove
        self.idx_to_keep = idx_to_keep

    @classmethod
    def _filter_out_syms_exclude_idx(cls, self):
        """Filter out symbols, drop idx from _stopwords_other_exclude."""
        df_symsk = (self.df_syms[self.df_syms.index
                    .get_level_values(0)
                    .isin(self.idx_to_keep)]
                    .copy())
        df_syms_count = (df_symsk[~df_symsk[0]
                         .isin(self.syms_to_remove)][0]
                         .value_counts()
                         .copy())

        if self.verbose:
            print("Df of symbols counts available at self.df_syms_count")
        return df_syms_count
