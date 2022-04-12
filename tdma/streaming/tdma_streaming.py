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
    from scripts.dev.multiuse.class_methods import ClsHelp
    from scripts.dev.multiuse.help_class import write_to_parquet, getDate, baseDir, help_print_arg
except ModuleNotFoundError:
    from tdma.streaming.streaming_params import TdmaStreamingParams
    from tdma.streaming.streaming_login import TdmaStreamingLoginParams
    from multiuse.class_methods import ClsHelp
    from multiuse.help_class import write_to_parquet, getDate, baseDir, help_print_arg


# %% codecell


class TdmaStreaming(TdmaStreamingLoginParams, TdmaStreamingParams, ClsHelp):
    """For the actual streaming."""

    def __init__(self, **kwargs):
        self._ts_get_class_vars(self, **kwargs)
        self._instantiate_streaming_logins(self, **kwargs)
        self._instantiate_streaming_params(self, **kwargs)
        if not self.check_vars_before_stream:
            self._start_tdma_streaming(self, self.uri, self.request_list)

    def __call__(self, **kwargs):
        self._ts_get_class_vars(self, **kwargs)
        self._start_tdma_streaming(self, self.uri, self.request_list)

    @classmethod
    def _ts_get_class_vars(cls, self, **kwargs):
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
        fdir = Path(baseDir().path, 'tdma', 'test_dump1')
        dt = getDate.query('iex_close')
        fpaths = ({
            'QUOTE': fdir.joinpath(f'quote_{dt}.parquet'),
            'OPTION': fdir.joinpath(f'option_{dt}.parquet'),
            'TIMESALE_EQUITY': fdir.joinpath(f'timesale_equity_{dt}.parquet'),
            'TIMESALE_OPTIONS': fdir.joinpath(f'timesale_options_{dt}.parquet'),
            'ACTIVES_OTCBB': fdir.joinpath(f'actives_otcbb_{dt}.parquet'),
            'ACTIVES_OPTIONS': fdir.joinpath(f'actives_options_{dt}.parquet')
        })

        if self.verbose:
            help_print_arg(f"Tdma - streaming fpaths - {str(fpaths)}")

        for rq in request_list:
            rq = {"request": [rq]}

        async for ws in websockets.connect(uri):
            try:
                a = await ws.send(json.dumps(self.request_login))
                print(str(await ws.recv()))
                for rq in request_list:
                    sa = await ws.send(json.dumps(rq))
                    print(str(await ws.recv()))

                msg = await self._process_message(self, ws, fdir, dt, fpaths)
            except websockets.ConnectionClosed:
                break

    @classmethod
    async def _process_message(cls, self, ws, fdir, dt, fpaths):
        """Process message and write to dataframe."""
        while True:
            data = await ws.recv()
            try:
                if self.verbose:
                    help_print_arg(str(data))

                data = json.loads(data)
                if data.get('notify', False) and not self.verbose:
                    help_print_arg(str(data))
                    continue
                elif data.get('response', False) and not self.verbose:
                    help_print_arg(str(data))
                    continue
                elif 'headline' in data.keys():
                    df = pd.json_normalize(data)
                    # kwargs = {'cols_to_drop': 'id'}
                    # write_to_parquet(df, self.fpath, combine=True, **kwargs)
                elif 'data' in data.keys():

                    if len(data['data']) == 1:
                        df_all = ((pd.json_normalize(
                                   data, meta=[['data', 'timestamp']],
                                   record_path=['data', ['content']],
                                   errors='ignore')).set_index('key'))
                        df_new = (df_all.groupby(level=0)
                                        .ffill()
                                        .reset_index().copy())

                        service = data['data'][0]['service']
                        # Check if service in in fpaths.keys
                        if service not in fpaths.keys() and self.verbose:
                            help_print_arg(f"Service {service} not in fpaths")
                        fpath = fpaths.get(service, fdir.joinpath(f"{service.lower()}_{dt}.parquet"))
                        write_to_parquet(df_new, fpath, combine=True)

                        if self.verbose:
                            help_print_arg(f"Wrote single length data service {service} to fpath {str(fpath)}")
                    else:
                        for n, row in enumerate(data):
                            df_service = (pd.json_normalize(
                                       data['data'][n], meta=['timestamp'],
                                       record_path=['content'],
                                       errors='ignore')
                                       .copy())

                            service = data['data'][n]['service']
                            if service not in fpaths.keys() and self.verbose:
                                help_print_arg(f"Service {service} not in fpaths")
                            fpath = fpaths.get(service, fdir.joinpath(f"{service.lower()}_{dt}.parquet"))
                            write_to_parquet(df_service, fpath, combine=True)

                            if self.verbose:
                                help_print_arg(f"Wrote service {service} to fpath {str(fpath)}")
                else:
                    if not self.verbose:
                        help_print_arg(f"""No tdma streaming method matched
                                        - data: {str(data)}""")
                    continue
            except AttributeError as ae:
                if self.verbose:
                    self.elog(self, ae, text=data, verbose=self.verbose)

                try:
                    df = pd.DataFrame(json.load(BytesIO(data)))
                    # kwargs = {'cols_to_drop': 'id'}
                    # write_to_parquet(df, self.fpath, combine=True, **kwargs)
                except Exception as e:
                    self.elog(self, e, text=f"json.load(BytesIO(data)) data: {data}", verbose=self.verbose)
                continue
            except json.JSONDecodeError as jde:
                self.elog(self, jde, verbose=self.verbose)
                continue
            except Exception as e:
                self.elog(self, e, verbose=self.verbose)
                continue

    # Cancel local streaming task
    @staticmethod
    def cancel_local_background_task():
        """Cancel asyncio local background task."""
        pending = list(asyncio.all_tasks())
        help_print_arg(f"asyncio background tasks: {str(pending)}")

        tdma_streaming_task = []
        for n, tk in enumerate(pending):
            if 'tdma_streaming' in str(pending[n]):
                tdma_streaming_task.append(pending[n])
                break

        if tdma_streaming_task:
            tdma_streaming_task = tdma_streaming_task[0]
            tdma_streaming_task.cancel()
