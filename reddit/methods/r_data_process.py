"""Reddit data process."""
# %% codecell

from praw.models import MoreComments

import pandas as pd

try:
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg


# %% codecell


class RedditDataProcess():
    """Subreddit info."""
    # self.r_sub should be passed - this class gets instantiated in RedditAPI
    # self.df_dp = dataframe _ data process - consistent between calls

    # Method dict for higher level routing, where rdp = RedditDataProcess
    rdp_dict = ({
        'ref_subs': 'rdp',
        'sub_comments': 'rdp'
    })

    def __init__(self, method, **kwargs):
        if method == 'ref_subs':
            self.df_dp = self._rdp_ref_subs_clean(self, **kwargs)
        elif method == 'sub_comments':
            self.df_dp = self._rdp_sub_comment_extract(self)

    @classmethod
    def _rdp_ref_subs_clean(cls, self, **kwargs):
        """Clean subreddit info for local storage."""
        self.r_sub._fetch()
        df_sub = pd.json_normalize(self.r_sub.__dict__)
        cols_sub = (['display_name', '_path', 'title', 'active_user_count',
                     'accounts_active', 'subscribers', 'name', 'quarantine',
                     'public_description', 'created', 'wls', 'subreddit_type',
                     'id', 'user_is_moderator', 'description',
                     'url', 'created_utc'])

        # Test for columns in common - print if none or not right
        cols_common = df_sub.columns.intersection(pd.Index(cols_sub)).tolist()
        if not cols_common or len(cols_common) < len(cols_sub):
            help_print_arg(f"RDP: {str(df_sub.columns)} {str(cols_common)}")
        df_dp = df_sub[cols_sub].copy()

        return df_dp

    @classmethod
    def _rdp_sub_comment_extract(cls, self):
        """Extract comments from subreddit."""
        sub_comments = self.r_sub.comments()
        cdict_list = []
        for comment in sub_comments:
            if isinstance(comment, MoreComments):
                continue
            cdict_list.append(comment.__dict__)
            # print(top_level_comment.body)

        df_cs = pd.json_normalize(cdict_list)
        cols_to_keep = (['subreddit_id', 'link_title', 'ups',
                         'link_author', 'id', 'archived', 'author',
                         'num_comments', 'parent_id', 'score', 'link_url',
                         'author_fullname', 'controversiality', 'body',
                         'downs', 'author_flair_richtext',
                         'body_html', 'can_gild', 'link_id', 'permalink',
                         'link_permalink', 'name', 'created', 'created_utc'])

        # Test for columns in common - print if none or not right
        cols_common = df_cs.columns.intersection(pd.Index(cols_to_keep)).tolist()
        if (not cols_common) or (len(cols_common) < len(cols_to_keep)):
            help_print_arg(f"RDP: {str(df_cs.columns)} {str(cols_common)}")

        df_dp = df_cs[cols_to_keep].copy()
        df_dp['author_name'] = df_dp['author'].astype('str')
        return df_dp

# %% codecell
