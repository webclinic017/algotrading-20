flex_oi_url = f"https://marketdata.theocc.com/flex-reports?reportType=OI&optionType=E&reportDate={report_date.strftime('%Y%m%d')}"
flex_oi_get = requests.get(flex_oi_url)

len(flex_oi_get.content)
print(CnM.from_bytes(flex_oi_get.content).best().first())


occ_oi = pd.read_csv(BytesIO(flex_oi_get.content), escapechar='\n', delimiter=',', delim_whitespace=True, skipinitialspace=True)
# Store columns names from row data
occ_oi_cols = occ_oi.iloc[5].values
# Rename the 8th column to what looks like volume
occ_oi_cols[8] = 'OI'
occ_oi_cols[7] = 'MARK'
occ_oi_cols[6] = 'VOLUME?'
occ_oi.columns = occ_oi_cols

# Exclude all rows that don't contain C and P in the P/C column
occ_oi = occ_oi[occ_oi['P/C'].isin(['C', 'P'])]
occ_oi.dropna(axis=1, how='all', inplace=True)
occ_oi['STYLE'] = occ_oi['SYMBOL'].str[0]
occ_oi['SYMBOL'] = occ_oi['SYMBOL'].str[1:]
occ_oi['expirationDate'] = occ_oi['YR'] + occ_oi['MO'] + occ_oi['DAY']


# Reset the index inplace
occ_oi.reset_index(drop=True, inplace=True)
# Load base_directory (derivatives data)
base_dir = f"{Path(os.getcwd()).parents[0]}/data/derivatives/occ_dump"
fname = f"{base_dir}/equity_oi_{report_date}"

# Write to local json file
occ_oi.to_json(fname)

occ_oi.columns

occ_oi.head(50)


# %% codecell
#######################################################################################
flex_url = f"https://marketdata.theocc.com/flex-reports?reportType=PR&optionType=E&reportDate=20210212"
flex_get = requests.get(flex_url)
occ_prices = pd.read_csv(BytesIO(flex_get.content), escapechar='\n', delimiter=',', delim_whitespace=True, skipinitialspace=True)

occ_prices.head(10)
occ_prices.reset_index(inplace=True)

occ_prices['level_1'].value_counts()


occ_flex_oi.occ_df.head(10)

col_row = occ_prices[occ_prices['level_0'] == 'SYMBOL'].head(1).index[0]
# Get column row names
occ_rc_cols = occ_rc.iloc[col_row].values
# Rename the 8th column to what looks like volume
occ_rc_cols[8] = 'OI'
occ_rc_cols[7] = 'MARK'
occ_rc_cols[6] = 'VOLUME?'
occ_rc.columns = occ_rc_cols
occ_rc.rename(columns={'C': 'SIDE', 'DA': 'DAY'}, inplace=True)


occ_cols = occ_prices.iloc[2, :].values
occ_prices.columns = occ_cols

occ_prices = occ_prices[~occ_prices['SYMBOL'].isin(['SYMBOL', '+OPTIONS', '*****', 'P', 'PC'])]
occ_prices['STYLE'] = occ_prices['SYMBOL'].str[0]
occ_prices['SYMBOL'] = occ_prices['SYMBOL'].str[1:]

occ_prices.dropna(axis=1, how='all', inplace=True)
occ_prices['expirationDate'] = occ_prices['YEAR'] + occ_prices['MO'] + occ_prices['DA']

occ_cols_old = [col for col in occ_prices.columns.values]

n = 0
new_price_names = ['STRIKE', 'UNDERLYING', 'MARK']
for col_n, col in enumerate(occ_cols_old):
    if col == 'PRICE':
        occ_cols_old[col_n] = new_price_names[n]
        n = n + 1

occ_prices.columns = occ_cols_old

# occ_prices.drop(occ_prices.tail(1).index, inplace=True)
# occ_prices = occ_prices[~occ_prices['level_0'].isin(['P'])]
# occ_prices = occ_prices[occ_prices.iloc[:, 7].notnull()]
