"""Master methods class."""
# %% codecell

try:
    from scripts.dev.reddit.methods.r_data_process import RedditDataProcess
    from scripts.dev.reddit.methods.subreddit_authors import RedditAuthors
    from scripts.dev.reddit.methods.helpers import RedditHelpers
    from scripts.dev.multiuse.help_class import help_print_arg, write_to_parquet
except ModuleNotFoundError:
    from reddit.methods.r_data_process import RedditDataProcess
    from reddit.methods.subreddit_authors import RedditAuthors
    from reddit.methods.helpers import RedditHelpers
    from multiuse.help_class import help_print_arg, write_to_parquet


# %% codecell


class RedditMethods(RedditDataProcess):
    """Base class for all reddit methods."""
    # Gets imported into RedditAPI and instantiated there
    rmethods_dict = ({})

    def __init__(self, method, **kwargs):
        self._get_class_vars(self, method, **kwargs)
        self.fpath = self._methods_get_fpath(self, method, **kwargs)
        self._methods_instantiate_r_data_process(self, method, **kwargs)
        self._write_to_parquet(self, method, **kwargs)

    @classmethod
    def _get_class_vars(cls, self, method, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose')
        if self.verbose:
            help_print_arg(f"RedditMethods: {str(method)}")

    @classmethod
    def _methods_get_fpath(cls, self, method, **kwargs):
        """Get relevant fpath for method."""
        fpath = RedditHelpers._get_path(method, **kwargs)
        return fpath

    @classmethod
    def _methods_instantiate_r_data_process(cls, self, method, **kwargs):
        """Instantiate RedditDataProcess."""
        RedditDataProcess.__init__(self, method, **kwargs)
        # Merge dictionary methods
        self.rmethods_dict = self.rmethods_dict | self.rdp_dict

    @classmethod
    def _methods_instantiate_authors(cls, self, method, **kwargs):
        """Instantiate subreddit authors class."""
        RedditAuthors.__init__(self, method=method, **kwargs)
        # Merge dictionary methods
        self.rmethods_dict = self.rmethods_dict | self.authors_dict

    @classmethod
    def _write_to_parquet(cls, self, method, **kwargs):
        """Write to parquet with optional space for more params."""
        self.df_write = self.df_dp.copy()
        # Create another dataframe to modify before writing, if needed
        if not self.skip_write:
            # Specify columns to drop_duplicates on
            if method == 'ref_subs':
                kwargs['cols_to_drop'] = ['name']
            elif method == 'sub_comments':
                kwargs['cols_to_drop'] = ['id']
                (self.df_write.drop(columns=['author', 'edited'],
                 inplace=True, errors='ignore'))
            elif method == 'sub_authors':
                kwargs['cols_to_drop'] = ['id']
                (self.df_write.drop(columns=['author'],
                 inplace=True, errors='ignore'))
            # Write to parquet
            write_to_parquet(self.df_write, self.fpath, combine=True, **kwargs)


# %% codecell
