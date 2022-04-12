"""Class methods + helpers."""
# %% codecell
import inspect

try:
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import help_print_arg

# %% codecell

class ClsHelp():
    # sf is the class object that's calling ClsHelp

    def elog(self, sf, e, text=None, verbose=False):
        """Error log."""
        name = self.cnames(sf)
        ename = self.ename(e)
    
        verbose = getattr(sf, 'verbose')
        if verbose:
            help_print_arg(f"{name} {ename} {str(text)}")

    def isp(self, sf, e):
        name = self.cnames(sf)
        ename = self.ename(e)
        isps = self._elog_test()
        return isps

    @staticmethod
    def cnames(sf, **kwargs):
        """Get name of function, class."""
        isp = inspect.stack()
        name = f"{type(sf).__name__}.{isp[1][3]}"

        if 'elog' in name:
            name = f"{type(sf).__name__}.{isp[2][3]}"
        return name

    @staticmethod
    def ename(e):
        """Custom error logging."""
        error = f"{type(e).__name__} {str(e)}"
        return error

    @staticmethod
    def _elog_test():
        """Elog test."""
        isp = inspect.stack()
        return isp


# %% codecell
