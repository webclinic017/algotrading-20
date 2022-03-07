"""Analysts Master Class."""
# %% codecell

try:
    from scripts.dev.data_collect.analysts.earnings.scraped_dates import scraped_ee_dates
    from scripts.dev.data_collect.analysts.helpers.paths import AnalystRecsPaths
    from scripts.dev.data_collect.analysts.prices.bz_data import get_hist_bz_ratings
except ModuleNotFoundError:
    from data_collect.analysts.earnings.scraped_dates import scraped_ee_dates
    from data_collect.analysts.helpers.paths import AnalystRecsPaths
    from data_collect.analysts.prices.bz_data import get_hist_bz_ratings

# %% codecell


class AnalystsMaster(AnalystRecsPaths):
    """Master class for analyst functions/ops."""

    def __init__(self, earnings=False, prices=False, hist=False, **kwargs):
        AnalystRecsPaths.__init__(self)

        if prices:
            self._get_prices(self, prices, **kwargs)
        elif earnings:
            self._get_earnings(self, earnings, hist, **kwargs)

    @classmethod
    def _get_prices(cls, self, prices, **kwargs):
        """Get analyst prices or run historical function."""
        get_hist_bz_ratings()

    @classmethod
    def _get_earnings(cls, self, earnings, hist, **kwargs):
        """Get analyst earnings estimates or historical estimates."""
        if hist:
            scraped_ee_dates(hist=True)
        else:
            scraped_ee_dates()


# %% codecell
