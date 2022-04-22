"""Celery batch processes."""
# %% codecell

try:
    from scripts.dev.remote_tasks.celery.celery_api import CeleryAPI
    from scripts.dev.api import serverAPI
except ModuleNotFoundError:
    from remote_tasks.celery.celery_api import CeleryAPI
    from api import serverAPI

# %% codecell


class CeleryBatchProcesses():
    """Celery batch processes."""

    def __init__(self, method, **kwargs):
        if method == 'restart_stream':
            self._cbp_restart_stream(self, **kwargs)

    @classmethod
    def _cbp_restart_stream(cls, self, **kwargs):
        """Stream worker: stop all running tasks and restart them."""
        # Remove all active/reserved tasks in queue
        # TDMA Stream + Apca real time news
        CeleryAPI.clear_all_active_reserved()
        #############################################################
        # To start a new news streaming session, remotely
        CeleryAPI(check_streaming=True, verbose=True)
        # TDMA Streaming
        serverAPI('redo', val='stream_tdma_streaming')
