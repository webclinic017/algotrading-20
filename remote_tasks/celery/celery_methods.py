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
    # Instantiated from celery_api

    def __init__(self, **kwargs):
        self._cm_apca_streaming(self, self.df_ar, **kwargs)
        self._cm_cancel_worker_queue(self, self.df_ar, **kwargs)

    @classmethod
    def _cm_apca_streaming(cls, self, df_ar, **kwargs):
        """Check that Apca real time news is streaming."""
        # Assume that self.df_ar (active/reserved) is passed in self
        start_apca_news_streaming = False
        streaming_condition = ''
        ans_name = 'app.tasks_stream.apca_news_streaming'

        if df_ar.empty:  # If active/reserved empty then start news_streaming
            start_apca_news_streaming = True
            streaming_condition = 'Empty Active/Reserved'
        else:  # Check if apca news streaming is in active/reserved dataframe
            ans = df_ar[df_ar['name'] == ans_name]
            if ans.empty:
                start_apca_news_streaming = True
                streaming_condition = f"Task {ans_name} not in df_ar"

        if start_apca_news_streaming and self.check_streaming:
            try:
                # Then start real time news streaming
                serverAPI('redo', val='apca_news_streaming')
            except ValueError:
                pass

        if self.verbose and streaming_condition:
            help_print_arg(f"_cm_apca_streaming {streaming_condition}")

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
