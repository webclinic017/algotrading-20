"""Methods for Celery (remote task queues)."""
# %% codecell
import requests

try:
    from scripts.dev.api import serverAPI
    from scripts.dev.multiuse.api_helpers import RecordAPICalls
    from scripts.dev.multiuse.help_class import help_print_arg
except ModuleNotFoundError:
    from api import serverAPI
    from multiuse.api_helpers import RecordAPICalls
    from multiuse.help_class import help_print_arg


# %% codecell


class CeleryMethods():
    """Class for implementing celery methods."""
    # Instantiated from celery_api. df_ar gets inherited

    def __init__(self, **kwargs):
        self._cm_apca_streaming(self, self.df_ar, **kwargs)
        self._cm_cancel_worker_queue(self, self.df_ar, **kwargs)

    @classmethod
    def _cm_apca_streaming(cls, self, df_ar, **kwargs):
        """Check that Apca real time news is streaming."""
        # Assume that self.df_ar (active/reserved) is passed in self
        start_apca_news_streaming, start_tdma_stream = False, False
        # Streaming name base
        sn_base, streaming_conds = 'app.tasks_stream', []

        # Check for active/reserved tasks
        if df_ar.empty:  # If active/reserved empty then start news_streaming
            start_apca_news_streaming = start_tdma_stream = True
            streaming_conds.append('Empty Active/Reserved')
        else:  # Check if apca news streaming is in active/reserved dataframe
            ans = df_ar[df_ar['name'] == f'{sn_base}.apca_news_streaming']
            if ans.empty:
                start_apca_news_streaming = True
                streaming_conds.append("apca_news_streaming not in df_ar")
            tds = df_ar[df_ar['name'] == f'{sn_base}.stream_tdma_streaming']
            if tds.empty:
                start_tdma_stream = True
                streaming_conds.append("stream_tdma_streaming not in df_ar")

        # Check if we shoudl initiate streams or just print output
        if self.check_streaming:
            try:
                if start_apca_news_streaming:
                    # Then start real time news streaming
                    serverAPI('redo', val='apca_news_streaming')
                if start_tdma_stream:
                    # Start tdma price/options streaming
                    serverAPI('redo', val='stream_tdma_streaming')
            except ValueError:
                pass

        # Automatically print output if not starting remote streams
        if (self.verbose and streaming_conds) or not self.check_streaming:
            help_print_arg(f"_cm_apca_streaming {str(streaming_conds)}")

    @classmethod
    def _cm_cancel_worker_queue(cls, self, df_ar, **kwargs):
        """Cancel worker tasks."""
        # Var is held under self.cancel_worker_tasks
        # Can take values of worker or just 'all'

        if self.cancel_worker_tasks:
            # Create queue list to match against active/reserved tasks
            if self.cancel_worker_tasks == 'all':
                queues = df_ar['delivery_info.routing_key'].tolist()
            else:
                queues = [self.cancel_worker_tasks]

            pdict = {}
            for index, row in df_ar.iterrows():
                # If routing key matches passed queue (or all)
                if row['delivery_info.routing_key'] in queues:
                    # if row['acknowledged']:  # Assume task is running
                    #    url = f"{self.burl}/task/abort/{row['id']}"
                    # else:  # Assume task is reserved
                    url = f"{self.burl}/task/revoke/{row['id']}"
                    true = True
                    params = {'terminate': true}

                    post = requests.post(url, auth=self.auth, params=params)
                    # Record api calls for celery
                    RecordAPICalls(post, 'celery')
                    # Store result in dictionary
                    pdict[url] = post
                    if post.status_code != 200:
                        help_print_arg(f"{post.status_code} {post.content}")

            if self.verbose:  # Print to console
                for key, val in pdict.items():
                    print(f"{key} {str(val.json())}")
            # Store post responses in class dict
            self.pdict = pdict
