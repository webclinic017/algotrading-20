"""Workbook for SEC EDGAR Database."""

# %% codecell
########################################
import xml.etree.ElementTree as ET
import requests
import pandas as pd
from charset_normalizer import CharsetNormalizerMatches as CnM
from io import BytesIO
import importlib
import sys

from data_collect.sec_routines import form_4, secCompanyIdx, secMasterIdx
importlib.reload(sys.modules['data_collect.sec_routines'])
from data_collect.sec_routines import form_4, secCompanyIdx, secMasterIdx

from api import serverAPI
from multiuse.help_class import baseDir, getDate
importlib.reload(sys.modules['multiuse.help_class'])
from multiuse.help_class import baseDir, getDate

# Display max 50 columns
pd.set_option('display.max_columns', 100)
# Display maximum rows
pd.set_option('display.max_rows', 50)
# %% codecell
########################################

url = "https://www.sec.gov/Archives/edgar/daily-index/2021/QTR2/sitemap.20210426.xml"
get = requests.get(url)

fpath = '/Users/unknown1/Algo/data/sec/raw/daily_index/2021/20210426'

f = open(fpath, 'wb')
f.write(get.content)
f.close()

print(CnM.from_bytes(get.content).best().first())

root = ET.fromstring(get.content.decode('UTF-8'))



# %% codecell
#####################################################################

data = []
for i, child in enumerate(root):
    data.append([subchild.text for subchild in child])

df = pd.DataFrame(data)  # Write in DF
df.columns = ['url', 'lastmod', 'changefreq', 'priority']

# Can get rid of changefreq and priority as they're constants
cols_to_add = ['cik', 'suffix']
df[cols_to_add] = df['url'].str.split(pat="/", expand=True).iloc[:,-2:]
df['cik'] = df['cik'].astype('uint32')

# %% codecell
#####################################################################

all_symbols = serverAPI('all_symbols').df

all_syms_df = all_symbols.copy(deep=True)
all_syms_df.dropna(axis=0, subset=['cik'], inplace=True)
all_syms_df['cik'] = all_syms_df['cik'].astype('uint32')

mod_syms_cols = ['cik', 'figi', 'name', 'symbol', 'type']
mod_syms = all_syms_df[mod_syms_cols].copy(deep=True)

# %% codecell
#####################################################################

sitemap_df = pd.merge(df, mod_syms, on='cik', indicator=True)
both_df = sitemap_df[sitemap_df['_merge'] == 'both'].copy(deep=True)
both_df.drop(columns=['_merge'], inplace=True)

# both_df[both_df['type'] == 'cs']['symbol'].value_counts().head(50)

all_wts = all_syms_df[all_syms_df['type'] == 'wt'].copy(deep=True)
all_wts.shape

all_wts['expDate'] = (pd.to_datetime(all_wts['name'].str[-11:-1],
                                     format='%d/%m/%Y', errors='coerce'))

all_wts['expDate'].isna().sum()

all_wts['name'].iloc[6]
all_wts['expDate'].head(25)

# %% codecell
#####################################################################
"""
D    - Basic information about the offering (Size/Date)
     - Filed up to 15 days after the first sale of securities
     - Can be a private placement to accredited investors
8Ks  - current reports
     - Earnings release, notice of delisting, acquisition
     - 8-K to announce significant events relevant to shareholders.
     - Have four business days to file an 8-K for most specified items.
     - Upon the election, appointment, or departure of specific officers
     - Report changes related to asset-backed securities
485BPOS  - Annual update filed by investment companies
10-K - Annual report
10-Q - Quarterly report
13F  - Quarterly report for institutions to disclose equity holdings
13G  - Beneficial ownership
13D  - Acitivist investor
5    -
4    - Changes in beneficial ownership for company insiders
     - Directors/officers, shareholders with more than 10% of the company's stock
3    - Initial statement of beneficial ownership
424B1  - New information not included in previous filings
424B2  - Primary offering on a delayed basis
       - Price of the security being offered, and its method of distribution
       - Preceded by Form S1, a general overview of the offering
         including the background of the company and its management team.
424B3  - Substantive facts or events that arose after the previous filing
425    - Disclosures related to proposed or upcoming merger
497    - Prospectus for investment companies using Form N-1A
N-Port - Funds with more than 1 billion in assets. Monthly reports
       - Funds other than money market funds/SBICs
       - Assets and liabilities
       - A number of portfolio-level quantitative risk metrics for funds with
         25% or more invested in debt instruments
       - Securities lending, including information on counterparties involved
       - Monthly total returns for each of the preceding three months
       - Flow information (net asset value of shares sold, shares sold in
         connection with re-investments of dividends and distributions,
         and shares redeemed or repurchased) for the preceding three months
       - Information on each investment in the portfolio
       - Miscellaneous securities
N-CEN  - Basic fund information
       - All funds complete parts A and B and file required attachments
       - All management companies, other than SBICs, will complete Part C
       - Closed-end funds and SBICs will complete Part D
       - ETFs, including those that are UITs, and exchange-traded managed
         funds will complete Part E
       - UITs will complete Part F

"""
from datetime import timedelta
from datetime import datetime
sec_burl = 'https://www.sec.gov/Archives'
master_url = "https://www.sec.gov/Archives/edgar/daily-index/2021/QTR2/master.20210427.idx"
master_get = requests.get(master_url)

hist_date = getDate.query('sec_master')
date_yest = (hist_date - timedelta(days=1))
sec_idx = secMasterIdx(hist_date=date_yest).df
sec_idx.head(10)

hist_date = '20210429'
datetime.strptime(hist_date, '%Y%m%d').date()
mast_df['Form Type'].value_counts().head(50)

all_cs = all_symbols[all_symbols['type'] == 'cs'].copy(deep=True)
all_cs.dropna(axis=0, subset=['cik'], inplace=True)
all_cs['cik'] = all_cs['cik'].astype('uint32')
all_cs_cik = all_cs['cik'].tolist()

mast_cs_df = mast_df[mast_df['CIK'].isin(all_cs_cik)].copy(deep=True)

mast_df = sec_idx.df.copy(deep=True)
mast_cs_4 = mast_df[mast_df['Form Type'] == '4']
mast_cs_4.iloc[0]['File Name']

test2_4_url = f"{sec_burl}/{mast_cs_4.iloc[0]['File Name']}"

from data_collect.sec_routines import form_4, secCompanyIdx, secMasterIdx
importlib.reload(sys.modules['data_collect.sec_routines'])
from data_collect.sec_routines import form_4, secCompanyIdx, secMasterIdx

test2_4_url

get = requests.get(test2_4_url)


split_test_xml = (get.content.split(bytes("<XML>", 'utf-8'))[1].decode('utf-8'))
split_test_xml = split_test_xml.split('</XML>')[0]

test2_4_url
test2_df = form_4(url=test2_4_url)
test2_df.head(10)


mast_cs_4.head(10)
# %% codecell
############################################################

mast_cs_8k = mast_cs_df[mast_cs_df['Form Type'] == '8-K'].copy(deep=True)
test_8k_url = f"{sec_burl}/{mast_cs_8k.iloc[0]['File Name']}"
test_8k_get = requests.get(test_8k_url)
test_8k_url
test_8k_df = form_4(test_8k_get)
test_8k_df.head(1)
split_test_xml = test_8k_get.content.split(bytes("<XML>", 'utf-8'))[1].decode('utf-8')
split_test_xml = split_test_xml.split('</XML>')[0]
string = split_test_xml.strip("\n\t ")
string = string.replace('\n', '')
split_test_root = ET.fromstring(string)

all_elements = split_test_root.findall(".//")
all_value_tags = split_test_root.findall(".//value")
all_value_parents = split_test_root.findall(".//value/..")

[elm.tag.split('}')[1] for elm in all_elements]

all_elements
# %% codecell
############################################################

# Get the first row of form 4 and see what we can find
test_4_url = f"{sec_burl}/{mast_cs_4.iloc[0]['File Name']}"

# %% codecell
############################################################

test1_4_url = f"{sec_burl}/{mast_cs_4.iloc[1]['File Name']}"
test1_4_get = requests.get(test1_4_url)

from data_collect.sec_routines import form_4, form_8k
importlib.reload(sys.modules['data_collect.sec_routines'])
from data_collect.sec_routines import form_4, form_8k


test_8k_df = form_8k(test_8k_get)

test1_4_df = form_4(test1_4_get)

# %% codecell
############################################################
test_4_get = requests.get(test_4_url)

split_xml = test_8k_get.content.split(bytes("<XML>", 'utf-8'))
split_xml_close = test_8k_get.content.split(bytes("</XML>", 'utf-8'))

split_xml = [sp.decode('utf-8') for sp in split_xml]
split_xml
for sp in range(1, len(split_xml)):


split_test_xml = test_4_get.content.split(bytes("<XML>", 'utf-8'))[1].decode('utf-8')
split_test_xml = split_test_xml.split('</XML>')[0]
string = split_test_xml.strip("\n\t ")
string = string.replace('\n', '')
split_test_root = ET.fromstring(string)

# Start with the value items to get the parents, then assign the values to the parents
all_value_tags = split_test_root.findall(".//value")
all_value_parents = split_test_root.findall(".//value/..")
all_elements = split_test_root.findall(".//")

tag_dict = {}
for tag,val in zip(all_value_parents, all_value_tags):
    tag_dict[tag.tag] = val.text

mod_elements = [elm for elm in all_elements if elm not in all_value_tags if elm not in all_value_parents]
for elm in mod_elements:
    try:
        tag_dict[elm.tag] = elm.text.strip()
    except AttributeError:
        pass

test_df = pd.DataFrame(tag_dict, index=range(1))

split_xbrl = test_8k_get.content.split(bytes("<XBRL>", 'utf-8'))
split_xbrl[1].decode('utf-8').split('span')


view_8k_url = 'https://www.sec.gov/Archives/edgar/data/1811856/000119312521079857/d155143d8k.htm'
view_8k_get = requests.get(view_8k_url)
from bs4 import BeautifulSoup
view_8k_soup = BeautifulSoup(view_8k_get.content.decode('utf-8'), 'html.parser')
warrant_table = view_8k_soup.find("b", string='Title of each class').find_parent('table').select('b')
cols, data = [], []
for wan, war in enumerate(warrant_table):
    if wan <= 2:
        cols.append(war.text.replace('\xa0', ' '))
    else:
        data.append(war.text.replace('\xa0', ' '))
cols
reshaped_data = np.reshape(np.array(data), (2, 3))
wt_table_df = pd.DataFrame(reshaped_data, columns=cols)
wt_table_df['price'] = (wt_table_df['Title of each class'].str
                        .rsplit(pat='$', n=1, expand=True).iloc[:, 1].str
                        .split(pat=' ', n=1).str[0]
                        .astype(np.float16))
renamed_cols = ['info', 'symbol', 'exch', 'price']
wt_table_df.columns = renamed_cols
reordered_cols = ['symbol', 'price', 'exch', 'info']
wt_table_df = wt_table_df[reordered_cols]


wt_table_df


# Find the i tag with warrants, get the closest p-tag that contains warrant information
view_8k_soup.find('i', string=['Warrants', ' Warrants', 'Warrants ']).find_parent('p').next_sibling.next_sibling.string.replace('\xa0', ' ').replace('\n', ' ')
view_8k_soup.find('i', string='Warrants ').find_parent('p').next_sibling.next_sibling.string
view_8k_soup.find_all('i')
print(CnM.from_bytes(view_8k_get.content).best().first())

mast_cs_4.shape
mast_cs_df.head(5)
print(CnM.from_bytes(master_get.content[0:10000]).best().first())

"""
https://www.sec.gov/Archives/edgar/daily-index/2021/QTR2/master.20210427.idx

"""

"""
ciks_df = all_syms_df[all_syms_df['cik'].isin(cik_list)]
ciks_df.shape
"""
