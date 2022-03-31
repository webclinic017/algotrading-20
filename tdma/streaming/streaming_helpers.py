"""Streaming helpers class."""
# %% codecell

import re

# %% codecell


class TdmaStreamingHelpers():
    """Tdma streaming helper methods."""

    @staticmethod
    def remove_exchar(rstring):
        """Remove excess characters (format for tdma)."""
        rcleaned = re.sub(r"[ ''\[\]]", '', string=str(rstring))
        return rcleaned
