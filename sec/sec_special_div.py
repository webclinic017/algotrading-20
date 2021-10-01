"""A file dedicated to finding SEC special dividend announcements."""
# %% codecell
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import requests
import os

from api import serverAPI

from multiuse.help_class import getDate
from multiuse.create_file_struct import makedirs_with_permissions
# %% codecell

# This is purely for name lookup
sec_ref_data = serverAPI('sec_ref').df
sec_ref_data.head(5)


sec_ref = serverAPI('sec_master_all').df
sec_df = sec_ref.copy()
sec_df['Date Filed'] = pd.to_datetime(sec_df['Date Filed'], format='%Y%m%d')

# sec_df.groupby('Date Filed').filter(lambda file: sec_df[file] == '8-K')

sec_df['Form Type'].value_counts()

bus_days = getDate.get_bus_days()
busdays_2021 = (bus_days[(bus_days['date'] >= '2021') &
                         (bus_days['date'] <= str(getDate.query('iex_eod')))])

dates_needed = list(set(busdays_2021['date'].tolist()) - set(sec_df['Date Filed'].tolist()))

sec_8k = sec_df[sec_df['Form Type'] == '8-K'].copy()
sec_8k['fpath_name'] = sec_8k['File Name'].str.split('/', expand=True)[3]

fpath_base = Path('../data/sec/filings_v2/')
fpath_base
path
Path(fpath_base)

path

n = 0
sec_url_p1 = "https://www.sec.gov/Archives/"
# %% codecell
header_dict = ({'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.sec.gov/edgar/search/',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:92.0) Gecko/20100101 Firefox/92.0'})

error_dict = {}
# %% codecell
for index, row in tqdm(sec_8k.reset_index().iterrows()):
    # First, if the file doesn't exist
    path = Path(fpath_base, str(row['CIK']), row['fpath_name'])
    if not os.path.isfile(path):
        # Every 8 iterations, pause for a second
        if n % 8 == 0:
            # if datetime.now() < (time_now + timedelta(seconds=1)):
            #    time.sleep(.5)
            #    time_now = datetime.now()
            n = 1
            # print('Got to 8 iterations')
            # break
            # time.sleep(1)
        else:
            n += 1
            sec_url = f"{sec_url_p1}{row['File Name']}"
            get = requests.get(sec_url, headers=header_dict)

            if get.status_code < 400:
                with open(path, "wb") as bf:
                    # Write bytes to file
                    bf.write(get.content)
                    bf.close()
            else:
                print('Error parsing file')
                error_dict[str(row['CIK'])] = get.content

# %% codecell
error_dict
Path('../data/sec/filings_v2/1070789').is_dir()


# %% codecell

# %% codecell
row['fpath_name']
list(fpath_base.glob('**/*.txt'))

for n in list(range(0, 9)):
    print(n % 8)
n = 7

sec_8k.head()

sec_8k.info()

jcs_cik = '0000022701'
sec_8k[sec_8k['CIK'] == jcs_cik]
sec_8k[sec_8k['Company Name'].str.contains('COMMUNICATIONS SYSTEMS INC')]
sec_url_p1 = "https://www.sec.gov/Archives/"
sec_url_p2 = 'edgar/data/22701/0000897101-21-000766.txt'
sec_url = f"{sec_url_p1}{sec_url_p2}"
sec_url
# %% codecell
header_dict = ({'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.sec.gov/edgar/search/',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:92.0) Gecko/20100101 Firefox/92.0'})

get = requests.get(sec_url, headers=header_dict)

test_path = Path('../data/test.txt').resolve()

# %% codecell
with open(test_path, "wb") as bf:
    # Write bytes to file
    bf.write(get.content)
    bf.close()

# %% codecell

with open(test_path, "rb") as bf:
    # Read bytes from file
    bf_test = bf.read()
    bf.close()

bf_test
# %% codecell

# So I can either try to parse this - I'd like to see other examples of this style

get_splits = get.content.decode('ISO-8859-1').split('<p')
special = [sp.split('>')[1].split('>')[0] for sp in get_splits if 'special dividend' in sp]

special

# I can make a dictionary with text for each day - "of" and "per share" - split for the price
# Then can look up the most recent closing price
# Create a dataframe for each day - special dividends

# %% codecell
fpath = Path('../data/sec/filings_v2')
fpath.resolve()

# %% codecell

# Create all directories
for cik in tqdm(sec_ref_data['CIK'].tolist()):
    makedirs_with_permissions(Path(fpath, str(cik)))


for cik in tqdm(sec_8k['CIK'].tolist()):
    makedirs_with_permissions(Path(fpath, str(cik)))

# %% codecell
# Store all 8ks
# max 8 requests per second
for row in sec_8k


# %% codecell

from time import time, sleep
while True:
    print('Running')
    sleep(60 - time() % 60)
	# thing to run
    print('Running')


def get_sec_docs_with_limit():

    sleep(1)

# %% codecell
sec_url_p2.split('/')[-1]

# Might need to make a special dividend df
spec_div_df = pd.DataFrame()
# CIK - company name - date - file path


sec_ref_data.head()



sec_df.info()

sec_df.head()

# %% codecell
