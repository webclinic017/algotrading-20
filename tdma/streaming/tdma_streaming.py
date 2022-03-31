"""TDMA Streaming."""
# %% codecell
from pathlib import Path
import json
from io import BytesIO

import asyncio
import websockets
import pandas as pd

try:
    from scripts.dev.tdma.streaming.streaming_login import TdmaStreamingLoginParams
    from scripts.dev.tdma.streaming.streaming_params import TdmaStreamingParams
    from scripts.dev.multiuse.help_class import write_to_parquet, getDate, baseDir, help_print_arg
except ModuleNotFoundError:
    from tdma.streaming.streaming_params import TdmaStreamingParams
    from tdma.streaming.streaming_login import TdmaStreamingLoginParams
    from multiuse.help_class import write_to_parquet, getDate, baseDir, help_print_arg


# %% codecell


class TdmaStreaming(TdmaStreamingLoginParams, TdmaStreamingParams):
    """For the actual streaming."""

    def __init__(self, **kwargs):
        self._tsp_get_class_vars(self, **kwargs)
        self._instantiate_streaming_logins(self, **kwargs)
        self._instantiate_streaming_params(self, **kwargs)
        if not self.check_vars_before_stream:
            self._start_tdma_streaming(self, self.uri, self.request_list)

    def __call__(self, **kwargs):
        self._tsp_get_class_vars(self, **kwargs)
        self._start_tdma_streaming(self, self.uri, self.request_list)

    @classmethod
    def _tsp_get_class_vars(cls, self, **kwargs):
        """Get class variables and unpack kwargs."""
        self.verbose = kwargs.get('verbose', False)
        self.testing = kwargs.get('testing', False)
        self.check_vars_before_stream = kwargs.get('check_vars_before_stream')

    @classmethod
    def _instantiate_streaming_logins(cls, self, **kwargs):
        """Instantiate the TdmaStreamingLoginParams class."""
        TdmaStreamingLoginParams.__init__(self, **kwargs)

    @classmethod
    def _instantiate_streaming_params(cls, self, **kwargs):
        """Instantiate TdmaStreamingParams class."""
        TdmaStreamingParams.__init__(self, self.df_gpf, **kwargs)

    @classmethod
    def _start_tdma_streaming(cls, self, uri, request_list):
        """Start td ameritrade streaming data."""
        try:
            loop = asyncio.get_running_loop()
            if loop and loop.is_running():
                print('Loop is running')
                tsk = loop.create_task(self._tdma_streaming(self, uri, request_list))
        except RuntimeError:
            print('Not running as loop')
            asyncio.run(self._tdma_streaming(self, uri, request_list))
            pass

    @classmethod
    async def _tdma_streaming(cls, self, uri, request_list):
        """TD Ameritrade Streaming."""
        fdir_test = Path(baseDir().path, 'tdma', 'test_dump')
        f_test = fdir_test.joinpath('test_tdma_all_write')
        for rq in request_list:
            rq = {"request": [rq]}

        async for ws in websockets.connect(uri):
            try:
                a = await ws.send(json.dumps(self.request_login))
                print(str(await ws.recv()))
                for rq in request_list:
                    sa = await ws.send(json.dumps(rq))
                    print(str(await ws.recv()))

                msg = await self._process_message(self, ws, f_test)
            except websockets.ConnectionClosed:
                break

    @classmethod
    async def _process_message(cls, self, ws, f_test):
        """Process message and write to dataframe."""
        while True:
            data = await ws.recv()
            try:
                if self.verbose:
                    help_print_arg(str(data))
                data = json.loads(data)
                if data.get('notify', False):
                    if self.verbose:
                        help_print_arg(str(data))
                    continue
                elif 'headline' in data.keys():
                    df = pd.json_normalize(data)
                    # kwargs = {'cols_to_drop': 'id'}
                    # write_to_parquet(df, self.fpath, combine=True, **kwargs)
                elif 'data' in data.keys():
                    df_all = ((pd.json_normalize(
                               data, meta=[['data', 'timestamp']],
                               record_path=['data', ['content']],
                               errors='ignore')).set_index('key'))
                    df_new = df_all.groupby(level=0).ffill().copy()
                    write_to_parquet(df_new, f_test, combine=True)
                else:
                    pass
            except AttributeError as ae:
                if self.verbose:
                    help_print_arg(str(ae))
                    help_print_arg(str(data))

                try:
                    df = pd.DataFrame(json.load(BytesIO(data)))
                    # kwargs = {'cols_to_drop': 'id'}
                    # write_to_parquet(df, self.fpath, combine=True, **kwargs)
                except Exception as e:
                    print(f"{str(e)} {type(e)}")
                continue
