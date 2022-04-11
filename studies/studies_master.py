"""Studies master class instead of individual funcs."""
# %% codecell

try:
    from scripts.dev.studies.add_study_cols import (add_gap_col, calc_rsi,
                                                    make_moving_averages,
                                                    add_fChangeP_col,
                                                    add_fHighMax_col,
                                                    first_cleanup_basic_cols)
    from scripts.dev.multiuse.class_methods import ClsHelp
except ModuleNotFoundError:
    from studies.add_study_cols import (add_gap_col, calc_rsi,
                                        make_moving_averages, add_fChangeP_col,
                                        add_fHighMax_col,
                                        first_cleanup_basic_cols)
    from multiuse.class_methods import ClsHelp
# %% codecell
# There are going to be different studies for different types
# Let's start with equities


class StudiesMaster():
    """Master class for TA Studies and else."""

    def __init__(self, df, method, **kwargs):
        self._sm_class_vars(self, method, **kwargs)
        if method == 'stock_eod':
            self.df_all = self._sm_stock_eod(self, df, **kwargs)

    @classmethod
    def _sm_class_vars(cls, self, method, **kwargs):
        """Get studies master class variables + kwargs."""
        self.verbose = kwargs.get('verbose')
        self.testing = kwargs.get('testing')

        self.method_dict = ({
            # Assumed daily stock EOD. Order matters here
            'stock_eod': ([first_cleanup_basic_cols, add_fChangeP_col,
                           add_gap_col, calc_rsi, make_moving_averages,
                           add_fHighMax_col])
        })

        if not self.method_dict.get(method):
            print(f"""StudiesMaster: method {str(method)} not
                      available in {str(self.method_dict.keys())}""")
            raise KeyError

    @classmethod
    def _sm_stock_eod(cls, self, df, **kwargs):
        """Methods for stock eod."""
        methods = self.method_dict['stock_eod']
        for m in methods:
            df = m(df).copy()

        cols_floats = df.select_dtypes(['float']).columns
        df[cols_floats] = df[cols_floats].astype('float64').round(3)

        return df
