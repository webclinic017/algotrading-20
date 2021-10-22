"""Master IEX stats."""
# %% codecell
import pandas as pd

try:
    from scripts.dev.data_collect.get_iex_stats import execute_iex_stats
    from scripts.dev.multiuse.symbol_ref_funcs import get_all_symbol_ref
    from scripts.dev.multiuse.help_class import help_print_arg, df_create_bins
except ModuleNotFoundError:
    from data_collect.get_iex_stats import execute_iex_stats
    from multiuse.symbol_ref_funcs import get_all_symbol_ref
    from multiuse.help_class import help_print_arg, df_create_bins

# %% codecell


class MasterIexStats():
    """Master function for initiating iex stats loop."""

    def __init__(self, testing=False):
        self.testing = testing
        cs_adr = self.get_all_syms(self)
        self.initiate_exec(self, cs_adr)

    @classmethod
    def get_all_syms(cls, self):
        """Get and return all syms to use."""
        # Type == 'cs' & 'adr'
        all_syms = get_all_symbol_ref()
        cs_adr = all_syms[all_syms['type'].isin(['cs', 'adr'])].copy()
        # Add bins with size 1000 each
        cs_adr = df_create_bins(cs_adr)
        # Appply lambda function for url construction
        cs_adr['url'] = cs_adr.apply(lambda row: f"/stock/{row['symbol']}/stats", axis=1)

        return cs_adr

    @classmethod
    def initiate_exec(cls, self, cs_adr):
        """Initiate execution of cs_adr loop through."""
        bins = cs_adr['bins'].unique().tolist()
        args = [cs_adr[cs_adr['bins'] == n] for n in iter(bins)]
        for arg in args:
            try:
                from app.tasks import execute_func
                kwargs = {'df': arg.to_json()}
                execute_func.delay('execute_iex_stats', **kwargs)
            except ModuleNotFoundError:
                execute_iex_stats(arg.to_json())
                help_print_arg('Execute yahoo options not found')

        # 15 minutes in the future, combine all company stats info
        # All previous symbols are assumed to have data at that point
        execute_func.apply_async(args=['combine_stats'], countdown=900)

# %% codecell
