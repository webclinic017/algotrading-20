"""Try importing all files from data_collect."""

# %% codecell
######################################
from data_collect.iex_class import readData, expDates, urlData, marketHolidays, companyStats

from data_collect.econ_class import yahooTbills

# %% codecell
######################################

# %% codecell
######################################
"""
load_dotenv()
base_url = os.environ.get("base_url")
payload = {'token': os.environ.get("iex_publish_api")}
fpath_aapl = "/Users/unknown1/Algo/data/StockEOD/2021/a/_AAPL.gz"
url = f"{base_url}/stock/{'AAPL'}/chart/{'5d'}"
get = requests.get(url, params=payload)

df_aapl = pd.read_json(StringIO(get.content.decode('utf-8')))
df_aapl.to_json(fpath_aapl)
"""
