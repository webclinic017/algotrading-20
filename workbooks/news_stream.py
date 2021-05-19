"""Workbook for streaming news."""

# %% codecell
#############################################

load_dotenv()
sym = 'OCGN'

url = 'https://cloud-sse.iexapis.com/stable/news-stream'
payload = ({'token': os.environ.get("iex_publish_api"),
            'symbols': sym})
get = requests.get(url, params=payload)

get = urlData(f"/stock/{sym}/news/last/{1}")
get.df

# %% codecell
#############################################
