"""Workbook for streaming news."""
# %% codecell
from pathlib import Path
import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv

try:
    from scripts.dev.multiuse.help_class import getDate, baseDir, help_print_arg
except ModuleNotFoundError:
    from multiuse.help_class import getDate, baseDir, help_print_arg

load_dotenv()
from multiuse.create_file_struct import make_hist_prices_dir
# %% codecell

# I have a few options here - if I try to request data at specified intervals,
# I risk using a ton of credits. I'm also not sure IEX is the best bet here.
# Does associated press have a free api? How about the wall street journal?

# %% codecell

path = Path(baseDir().path, 'tickers', 'my_syms.parquet')
my_syms = pd.read_parquet(path)
sym_list = my_syms['symbol'].tolist()
sym_list.append('SPY')

from api import serverAPI
all_syms = serverAPI('all_symbols').df
all_cs = all_syms[all_syms['type'] == 'cs'].head(500)
sym_list = all_cs['symbol'].tolist()

# %% codecell
len(sym_list)

# %% codecell
url = 'https://cloud-sse.iexapis.com/stable/news-stream'
# firehose_url = 'https://cloud-sse.iexapis.com/stable/news-stream'
payload = ({'token': os.environ.get("iex_publish_api"),
            'symbols': sym_list})
headers = {'Accept': 'text/event-stream'}
timeout = 600
running = True

r_list = []
r_fail = []

import json
from json import JSONDecodeError
from sseclient import SSEClient

# %% codecell
messages = SSEClient(firehose_url, headers=headers, params=payload, retry=100)

msg_list = [msg for msg in messages]

# %% codecell
payload = ({'token': os.environ.get("iex_publish_api")})

s = requests.Session()
resp = s.get(url, headers=headers, params=payload, stream=True, timeout=600)

resp_text = resp.text

# %% codecell
for msg in messages:
    try:
        if msg.data:
            r_list.append(msg.data)
    except JSONDecodeError:
        print(msg.dump())
        r_fail.append(msg.dump())

# %% codecell

resp_json = resp.json()

# %% codecell

# %% codecell
full_list = [f for f in r_list if f]
full_list

# %% codecell
url = 'https://cloud-sse.iexapis.com/stable/news-stream'
header_dict = {'Accept': 'text/event-stream'}

payload = ({'token': os.environ.get("iex_publish_api"),
            'symbols': sym_list})

s = requests.Session()

from datetime import datetime, date
dt = date.today()
base_path = Path(baseDir().path, 'news/historical', )

from math import inf
from time import sleep
import ssl
import urllib3
import logging
log = logging.getLogger(__name__)

# %% codecell


class StreamingNews():
    """Streaming news and writing to local file."""


    def __init__(self, *, chunk_size=512, daemon=False,
                 max_retries=inf, proxy=None, verify=True):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        self.chunk_size = chunk_size
        self.daemon = daemon
        self.max_retries = max_retries
        self.proxies = {"https": proxy} if proxy else {}
        self.verify = verify

        self.running = False
        self.session = None
        self.thread = None


    def _connect(self, method, endpoint, params=None, headers=None, body=None):
        self.running = True

        error_count = 0
        stall_timeout = 90
        network_error_wait = network_error_wait_step = 0.25
        network_error_wait_max = 16
        http_error_wait = http_error_wait_start = 5
        http_error_wait_max = 320
        http_420_error_wait_start = 60

        if self.session is None:
            self.session = requests.Session()

        url = 'https://cloud-sse.iexapis.com/stable/news-stream'

        try:
            while self.running and error_count <= self.max_retries:
                try:
                     with self.session.request(
                        method, url, params=params, headers=headers, data=body,
                        timeout=stall_timeout, stream=True,
                    ) as resp:
                        if resp.status_code == 200:
                            error_count = 0
                            http_error_wait = http_error_wait_start
                            network_error_wait = network_error_wait_step

                            self.on_connect()
                            if not self.running:
                                break

                            for line in resp.iter_lines(
                                chunk_size=self.chunk_size
                            ):
                                if line:
                                    self.on_data(line)
                                else:
                                    self.on_keep_alive()
                                if not self.running:
                                    break

                            if resp.raw.closed:
                                self.on_closed(resp)
                        else:
                            self.on_request_error(resp.status_code)
                            if not self.running:
                                break

                            error_count += 1

                            if resp.status_code == 420:
                                if http_error_wait < http_420_error_wait_start:
                                    http_error_wait = http_420_error_wait_start

                            sleep(http_error_wait)

                            http_error_wait *= 2
                            if http_error_wait > http_error_wait_max:
                                http_error_wait = http_error_wait_max

                except (requests.ConnectionError, requests.Timeout,
                        requests.exceptions.ChunkedEncodingError,
                        ssl.SSLError, urllib3.exceptions.ReadTimeoutError,
                        urllib3.exceptions.ProtocolError) as exc:
                    # This is still necessary, as a SSLError can actually be
                    # thrown when using Requests
                    # If it's not time out treat it like any other exception
                    if isinstance(exc, ssl.SSLError):
                        if not (exc.args and "timed out" in str(exc.args[0])):
                            raise

                    self.on_connection_error()
                    if not self.running:
                        break

                    sleep(network_error_wait)

                    network_error_wait += network_error_wait_step
                    if network_error_wait > network_error_wait_max:
                        network_error_wait = network_error_wait_max
        except Exception as exc:
            self.on_exception(exc)
        finally:
            self.session.close()
            self.running = False
            self.on_disconnect()




def disconnect(self):
        """Disconnect the stream"""
        self.running = False

def on_closed(self, response):
    """This is called when the stream has been closed by Twitter.
    Parameters
    ----------
    response : requests.Response
        The Response from Twitter
    """
    log.error("Stream connection closed by Twitter")

def on_connect(self):
    """This is called after successfully connecting to the streaming API.
    """
    log.info("Stream connected")

def on_connection_error(self):
    """This is called when the stream connection errors or times out."""
    log.error("Stream connection has errored or timed out")

def on_disconnect(self):
    """This is called when the stream has disconnected."""
    log.info("Stream disconnected")

for line in resp.iter_lines():
    decoded_line = line.decode('utf-8')
    print(json.loads(decoded_line))



df = pd.DataFrame.from_dict(resp.json(), orient='index').T
cols_to_dt = ['datetime', 'date', 'updated']
df[cols_to_dt] = df[cols_to_dt].apply(pd.to_datetime, unit='ms', errors='coerce')
df['symbol'] = df['key']


print(str(df['symbol']))




r = requests.get('https://httpbin.org/stream/20', stream=True)

for line in r.iter_lines():

    # filter out keep-alive new lines
    if line:
        decoded_line = line.decode('utf-8')
        print(json.loads(decoded_line))

# %% codecell
# Single line json object - needs to converted from a dict to a dataframe, stored with other news articles
base_path = Path(baseDir().path, 'news', 'historical')
make_hist_prices_dir(base_path)

# %% codecell
false = False
sample =  {
  "datetime":1624014000000,
  "hasPaywall":false,
  "headline":"COVID-19 vaccines for Taiwan to be bought by Apple suppliers TSMC and Foxconn",
  "image":"https://cloud.iexapis.com/v1/news/image/3FR4KFyB2Dg3Dv0hIwxSMxmGJRG87M1iscBgyeUkA5ud",
  "imageUrl":"https://i1.wp.com/9to5mac.com/wp-content/uploads/sites/6/2021/06/COVID-19-vaccines-for-Taiwan.jpg?resize=1200%2C628&quality=82&strip=all&ssl=1",
  "lang":"en",
  "provider":"CityFalcon",
  "qmUrl":"https://machash.com/9to5mac/309507/covid-19-vaccines-taiwan-to-bought-apple-suppliers-tsmc/?utm_campaign=cityfalcon&utm_medium=cityfalcon&utm_source=cityfalcon",
  "related":"AAPL",
  "source":"GlassWave",
  "summary":"Apple suppliers TSMC and Foxconn are working on buying millions of doses of COVID-19 vaccines for Taiwan. The plan is for the two companies to buy the vaccines, then donate them to the government for distribution to the people of the island nation. The indirect arrangement, where the government authorizes private companies to buy vaccines on behalf of the country, is designed to thwart Chinese interference . . . more . . . The post COVID-19 vaccines for Taiwan to be bought by Apple suppliers TSMC and Foxconn appeared first on 9to5Mac.",
  "url":"https://cloud.iexapis.com/v1/news/article/3FR4KFyB2Dg3Dv0hIwxSMxmGJRG87M1iscBgyeUkA5ud",
  "id":"NEWS",
  "key":"AAPL",
  "subkey":"3FR4KFyB2Dg3Dv0hIwxSMxmGJRG87M1iscBgyeUkA5ud",
  "date":1624014000000,
  "updated":1624014000000
  }
# %% codecell

df = pd.DataFrame.from_dict(sample, orient='index').T
cols_to_dt = ['datetime', 'date', 'updated']
df[cols_to_dt] = df[cols_to_dt].apply(pd.to_datetime, unit='ms', errors='coerce')
df['symbol'] = df['key']

# %% codecell



# %% codecell
