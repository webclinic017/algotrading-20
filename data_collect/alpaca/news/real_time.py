"""Alpaca real time news streaming."""
# %% codecell
import json
from pathlib import Path
import asyncio
import websockets
from io import BytesIO
import pandas as pd

try:
    from scripts.dev.multiuse.help_class import baseDir, getDate, write_to_parquet
    from scripts.dev.data_collect.alpaca.api_calls.apca_auth import ApcaAuth
except ModuleNotFoundError:
    from multiuse.help_class import baseDir, getDate, write_to_parquet
    from data_collect.alpaca.api_calls.apca_auth import ApcaAuth


# %% codecell


class ApcaNewsStream(ApcaAuth):
    """Apca real-time news stream with websockets."""

    # Url for websocket streaming
    uri = 'wss://stream.data.alpaca.markets/v1beta1/news'
    # All stock and crypto symbols
    subscribe_all = {"action": "subscribe", "news": ["*"]}
    # Unsubscribe
    unsubscribe_all = {"action": "unsubscribe", "news": ["*"]}

    def __init__(self, **kwargs):
        ApcaAuth.__init__(self, **kwargs)
        self.auth = self._create_auth_msg(self, **kwargs)
        self.fpath = self._create_fpath(self, **kwargs)
        self._start_news_streaming(self)

    @classmethod
    def _create_auth_msg(cls, self, **kwargs):
        """Create auth message."""
        auth = ({"action": "auth",
                 "key": self.headers['APCA-API-KEY-ID'],
                 "secret": self.headers['APCA-API-SECRET-KEY']})

        auth = json.dumps(auth)
        return auth

    @classmethod
    def _create_fpath(cls, self, **kwargs):
        """Construct and return fpath."""
        bpath = Path(baseDir().path, 'alpaca', 'news', 'real_time')
        dt = getDate.query('iex_eod')
        f_path = bpath.joinpath(f"_{str(dt.year)}.parquet")
        return f_path

    @classmethod
    def _start_news_streaming(cls, self):
        """instantiate news streaming function."""
        try:
            loop = asyncio.get_running_loop()
            if loop and loop.is_running():
                print('Loop is running')
                tsk = loop.create_task(self._news_streaming(self))
        except RuntimeError:
            asyncio.run(self._news_streaming(self))

    @classmethod
    async def _news_streaming(cls, self):
        """Asyncio news streaming."""
        async for ws in websockets.connect(self.uri):
            try:
                a = await ws.send(self.auth)
                print(str(await ws.recv()))
                sa = await ws.send(json.dumps(self.subscribe_all))
                print(str(await ws.recv()))

                msg = await self._process_message(self, ws)
            except websockets.ConnectionClosed:
                continue

    @classmethod
    async def _process_message(cls, self, ws):
        """Process message and write to dataframe."""
        while True:
            data = await ws.recv()
            try:
                data = json.loads(data)[0]
                if 'headline' in data.keys():
                    df = pd.json_normalize(data)
                    kwargs = {'cols_to_drop': 'id'}
                    write_to_parquet(df, self.fpath, combine=True, **kwargs)
            except AttributeError as ae:
                print(str(ae))
                print(str(data))

                try:
                    df = pd.DataFrame(json.load(BytesIO(data)))
                    kwargs = {'cols_to_drop': 'id'}
                    write_to_parquet(df, self.fpath, combine=True, **kwargs)
                except Exception as e:
                    print(f"{str(e)} {type(e)}")
                continue
