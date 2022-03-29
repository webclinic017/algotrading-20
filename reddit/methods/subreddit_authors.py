"""Subreddit authors, if there are any."""
# %% codecell

import pandas as pd
from tqdm import tqdm

try:
    from scripts.dev.reddit.methods.helpers import RedditHelpers
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from reddit.methods.helpers import RedditHelpers
    from multiuse.help_class import help_print_arg


# %% codecell


class RedditAuthors():
    """Class for getting reddit author details from comments."""

    authors_dict = ({
        'sub_authors': 'ra',
    })

    def __init__(self, **kwargs):
        method = kwargs.get('method', False)
        # Check if method matches. If so, proceed
        if method == 'sub_authors':
            df_comments = kwargs.get('df_comments', False)
            if not isinstance(df_comments, pd.DataFrame):
                df_comments = self._get_comments_df(self, **kwargs)

            self.df_authors_needed = self._check_for_authors_df(self, df_comments)
            self.df_authors_new = (self._get_new_authors_meta(self,
                                   self.df_authors_needed))
            self.df_dp = self._merge_old_new_authors(self)

    @classmethod
    def _get_comments_df(cls, self, **kwargs):
        """Get comments dataframe, if it exists."""
        f_sub_comments = RedditHelpers._get_path('sub_comments', **kwargs)
        df_comments = None
        if f_sub_comments.exists():
            df_comments = pd.read_parquet(f_sub_comments)

        return df_comments

    @classmethod
    def _check_for_authors_df(cls, self, df_comments):
        """Check for existing authors df. Reduce to unknown authors."""
        if 'author_name' not in df_comments.columns:
            df_comments['author_name'] = df_comments['author'].astype('str')
        # Check for local authors file
        f_sub_authors = RedditHelpers._get_path('sub_authors')
        df_authors_needed = df_comments
        if f_sub_authors.exists():
            df_authors = pd.read_parquet(f_sub_authors)

            df_authors_needed = (df_comments[~df_comments['author_name']
                                 .isin(df_authors['author_name'])]
                                 .copy())
        # Define author columns
        author_cols = (['author_name', 'author_fullname', 'author',
                        'link_author', 'num_comments'])
        df_authors_needed = df_authors_needed[author_cols].copy()
        return df_authors_needed

    @classmethod
    def _get_new_authors_meta(cls, self, df_authors_needed):
        """If this dataframe isn't empty, get new authors info."""
        author_list = []
        author_meta_list = []
        for index, row in tqdm(df_authors_needed.iterrows()):
            if row['author_fullname'] not in author_list:
                row['author']._fetch()
                author_meta_list.append(row['author'].__dict__)
                author_list.append(row['author_fullname'])

        # Drop columns after converting rows into dataframe
        cols_to_drop = (['_listing_use_sort', '_reddit', '_fetched',
                         'snoovatar_size', 'icon_img',
                         'pref_show_snoovatar', 'snoovatar_img'])
        df_authors_new = (pd.json_normalize(author_meta_list)
                            .rename(columns={'name': 'author_name'})
                            .drop(columns=cols_to_drop, errors='ignore'))

        return df_authors_new

    @classmethod
    def _merge_old_new_authors(cls, self):
        """Merge old and new author dataframes."""
        df_authors = (self.df_authors_needed.merge(
                      self.df_authors_new, on='author_name'))
        cols = df_authors.columns
        cols_overlap = cols[cols.str.contains('_y|_x')]
        if not cols_overlap.empty:
            help_print_arg(f"""Merge fail on reddit authors: cols
                               {str(cols_overlap)}.
                               Exiting and not writing locally""")
            self.skip_write = True
        # Return new df_authors dataframe
        return df_authors
