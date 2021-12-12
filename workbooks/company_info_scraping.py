from bs4 import BeautifulSoup

# %% codecell
# df_cleaned1.info()

try:
    url = 'http://www.crunchbase.com/organization/slack'
    headers = ({'Accept-Encoding': 'gzip, deflate, br', 'Cache-Control': 'no-cache', 'Host': 'www.crunchbase.com',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:92.0) Gecko/20100101 Firefox/92.0'})
    get = requests.get(url, headers=headers)
except Exception as e:
    print(e)


get.status_code
# %% codecell
bs = BeautifulSoup(get.text)

tag = bs.b
