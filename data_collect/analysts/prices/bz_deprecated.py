"""BZ Ratings (no longer used)."""
# %% codecell

# %% codecell


class BzRecs():
    """Get analyst recommendations."""

    """
    self.df : og df : str cols not converted
    self.con_df : converted column df
    self.benz_df : final dataframe before write_to_parquet
    self.path : path to write
    """

    def __init__(self):
        self._get_clean_data(self)
        self._object_cols_to_floats(self, self.df)
        self._write_to_parquet(self, self.con_df)

    @classmethod
    def _get_clean_data(cls, self):
        """Get and clean analyst recommendations."""
        rec_url = 'https://www.benzinga.com/analyst-ratings'
        df = pd.read_html(rec_url)[0]
        if df.empty:
            help_print_arg("BzRecs dataframe empty. Check url/request for failure")
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
        try:
            df.drop(columns=['Get Alert'], inplace=True)
        except KeyError:
            pass

        # Convert object columns into floats as needed
        df['Current price'] = df['Current price'].str.replace('$', '', regex=False)
        df['Upside/Downside'] = df['Upside/Downside'].str.replace('%', '', regex=False)
        df['prev_price'] = df['Price Target Change'].str.split('$').str[0]
        df['new_price'] = df['Price Target Change'].str.split('$').str[-2]
        self.df = df

    @classmethod
    def _object_cols_to_floats(cls, self, df):
        """Convert object columns to floats."""
        cols = ['Current price', 'Upside/Downside', 'prev_price', 'new_price']
        unwanted = string.ascii_letters + string.punctuation + string.whitespace + '\u2192'
        unwanted = unwanted.replace('.', '').replace('-', '')
        stringCols = df.dtypes[df.dtypes == 'object']

        for col in cols:
            if col in stringCols:
                df[col] = pd.to_numeric(df[col].str.strip(unwanted), errors='coerce')

        (df.rename(columns={'Date': 'date', 'Ticker': 'symbol',
                            'Upside/Downside': 'perc_change',
                            'Current price': 'mark'},
                   inplace=True))

        self.con_df = df.copy()

    @classmethod
    def _write_to_parquet(cls, self, con_df):
        """Write to parquet/concat if needed."""
        benz_all_df, benz_df = None, None

        bpath = Path(baseDir().path, 'company_stats', 'analyst_recs')
        path = Path(bpath, f"_{getDate.query('iex_close')}.parquet")
        path_all = Path(bpath, 'analyst_recs.parquet')

        if path.exists():
            df_old = pd.read_parquet(path)
            benz_df = (pd.concat([df_old, con_df])
                         .drop_duplicates(subset= ['date', 'symbol', 'Analyst Firm'])
                         .reset_index(drop=True))
        else:
            benz_df = con_df.copy()

        if path_all.exists():
            df_old = pd.read_parquet(path_all)
            benz_all_df = (pd.concat([df_old, con_df])
                             .drop_duplicates(subset=['date', 'symbol', 'Analyst Firm'])
                             .reset_index(drop=True))
        else:
            benz_all_df = con_df.copy()

        self.path = str(path)
        self.benz_df = benz_df.copy()
        write_to_parquet(benz_df, path)
        write_to_parquet(benz_all_df, path_all)
