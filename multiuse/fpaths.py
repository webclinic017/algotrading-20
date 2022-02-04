"""Fpath Dicts."""
# %% codecell
from pathlib import Path

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, help_print_arg
    from scripts.dev.multiuse.path_helpers import get_most_recent_fpath
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, help_print_arg
    from multiuse.path_helpers import get_most_recent_fpath

# %% codecell


class FindTheFpath():
    """Find/return fpaths for each of the data processes."""

    """
    var: self.fpath : fpath to read
    """

    def __init__(self, category=None, keyword=None):
        if category:
            self._display_options(self, category, keyword)
        if keyword:
            self._process_keyword(self, keyword)

    @classmethod
    def _display_options(cls, self, category, keyword):
        """Display categorical options."""
        cat_dict = ({
            'peers': FpathDicts.get_peers(),
            'refs': FpathDicts.symbol_ref_data(),
            'ticks': FpathDicts.intraday_tick(),
            'warrants': FpathDicts.warrants(),
            'company_stats': FpathDicts.company_stats(),
            'scans': FpathDicts.scans(),
            'sec': FpathDicts.sec(),
            'externals': FpathDicts.externals(),
            'stocktwits': FpathDicts.stocktwits(),
            'historical': FpathDicts.historical()
        })
        self.cat_dict = cat_dict

        if category in cat_dict:
            if keyword:
                if keyword in cat_dict[category]:
                    self.fpath = cat_dict[category][keyword]
                else:
                    self.options = cat_dict[category]
            else:
                help_print_arg('Could not find your keyword')

        else:
            help_print_arg('Could not find your category')
            self.options = cat_dict.keys()

    @classmethod
    def _process_keyword(cls, self, keyword):
        """Determine if keyword is valid or not."""
        pass


class FpathDicts():
    """Functions for returning fpaths when called."""

    @staticmethod
    def get_peers():
        """Get dict for peers."""
        bpath = Path(baseDir().path, 'ref_data', 'peer_list')
        peers = ({
            'all_correlations': bpath.joinpath('_corrlist_all.parquet'),
            'extremes': bpath.joinpath('_peers_extreme.parquet'),
            'peers_80': bpath.joinpath('_peers.parquet')
        })

        return peers

    @staticmethod
    def symbol_ref_data():
        """Symbol reference data dictionary."""
        bpath = Path(baseDir().path, 'ref_data', 'fresh')

        refs = ({
            # Which symbols are enabled for trading (most are)
            'enabled_syms': bpath.joinpath('iex_trading_syms.parquet'),
            'intl_exch': bpath.joinpath('intl_exchanges.parquet'),
            'us_exch': bpath.joinpath('us_exchanges.parquet'),
            # List of 800 different sector tags
            'iex_tags': bpath.joinpath('iex_tags.parquet'),
            # Roughly 42,000 mutual funds with identifiers
            'mutual_funds': bpath.joinpath('iex_mutual_funds.parquet')

        })

        return refs

    @staticmethod
    def intraday_tick():
        """Intraday tick data."""
        bpath_t = Path(baseDir().path, 'tickers', 'sectors')

        ticks = ({
            'sector_perf': get_most_recent_fpath(bpath_t, f_pre='performance'),
            'treasuries': Path(baseDir().path, 'economic_data', 'treasuries.parquet')
        })

        return ticks

    @staticmethod
    def warrants():
        """Warrant information/records."""
        bpath = Path(baseDir().path, 'tickers', 'warrants')

        warrants = ({
            'all': get_most_recent_fpath(bpath.joinpath('all')),
            'all_hist': get_most_recent_fpath(bpath.joinpath('all_hist')),
            'cheapest': get_most_recent_fpath(bpath.joinpath('cheapest')),
            'newest': get_most_recent_fpath(bpath.joinpath('newest')),
            'top_perf': get_most_recent_fpath(bpath.joinpath('top_perf')),
            'worst_pef': get_most_recent_fpath(bpath.joinpath('worst_perf'))
        })

        return warrants

    @staticmethod
    def company_stats():
        """Company stats."""
        bpath = Path(baseDir().path, 'company_stats')

        stats = ({
            'analyst_recs': bpath.joinpath('analyst_recs', '_2022.parquet'),
            'meta': get_most_recent_fpath(bpath.joinpath('meta', 'combined')),
            'stats': get_most_recent_fpath(bpath.joinpath('stats', 'combined'))
        })

        return stats

    @staticmethod
    def scans():
        """Scans for stocks/other items."""
        bpath = Path(baseDir().path, 'scans')

        scans = ({
            'top_vol': get_most_recent_fpath(bpath.joinpath('top_vol'))
        })

        return scans

    @staticmethod
    def externals():
        """External information sources."""
        bpath = Path(baseDir().path)

        externals = ({
            'daily_breaker': get_most_recent_fpath(bpath.joinpath('short', 'daily_breaker'), f_pre='nasdaq'),
            'halts': get_most_recent_fpath(bpath.joinpath('short', 'halts'))
        })

        return externals

    @staticmethod
    def sec():
        """SEC feeds."""
        bpath = Path(baseDir().path, 'sec')
        dt = getDate.query('iex_eod')
        yr = str(dt.year)

        sec = ({
            'rss': get_most_recent_fpath(bpath.joinpath('rss', yr)),
            'daily_idx': get_most_recent_fpath(bpath.joinpath('daily_index', yr)),
            'daily_idx_combined': bpath.joinpath('daily_index', '_all_combined.parquet')
        })

        return sec

    @staticmethod
    def stocktwits():
        """Stocktwits data."""
        bpath = Path(baseDir().path, 'stocktwits')

        stocktwits = ({
            'trending': get_most_recent_fpath(bpath.joinpath('trending'), f_pre='_')
        })

        return stocktwits

    @staticmethod
    def historical():
        """Historical EOD data."""
        bpath = Path(baseDir().path, 'StockEOD')

        historical = ({
            'combined': get_most_recent_fpath(bpath.joinpath('combined')),
            'combined_all': get_most_recent_fpath(bpath.joinpath('combined_all')),
            'combined_year': get_most_recent_fpath(bpath.joinpath('combined_year'))
        })

# %% codecell
