import requests
import pandas as pd

class PlacesClient:
    def __init__(self, token):
        self.base_url = 'https://data.cdc.gov/api/v3/views/'
        self.session = requests.Session()
        self.session.headers.update({
            'X-App-Token': token
        })

    def _make_request(self, url, params=None):
        """
        Make a get request to the API and return responses in JSON
        """
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"API Error: {e}")
            raise
    
    def _json_to_df(self, data):
        """
        Transform JSON data into pandas DataFrame.
        """
        df = pd.DataFrame(data)
        # remove the API's metadata
        df = df.drop([':id', ':version', ':created_at', ':updated_at'], axis=1, errors='ignore')
        # convert numeric variables
        numeric_cols = ['data_value', 'low_confidence_limit', 'high_confidence_limit', 'totalpopulation']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col])
        return df

    def get_county_data(self, release='2025'):
        """
        Retrieve county-level health-risk behaviors and health outcomes data from The CDC PLACES API.
        
        Parameters
        ----------
        release : string
            The version of release to retrieve from.

        Returns
        -------
        county_df : pandas DataFrame
            A dataframe containing information of county-level PLACES data
        
        Examples
        --------
        >>> df = get_county_data('2023')
        >>> df.head()
        """
        release_ids = {
            '2025': 'swc5-untb',
            '2024': 'fu4u-a9bh',
            '2023': 'h3ej-a9ec',
            '2022': 'duw2-7jbt',
            '2021': 'pqpp-u99h'
        }
        
        if not isinstance(release, str):
            raise TypeError("The release must be a string.")
        elif release not in release_ids:
            raise ValueError("This release version is not supported.")
        else:
            url = self.base_url + release_ids[release] + '/query.json'

        data = self._make_request(url)
        county_df = self._json_to_df(data)
        
        # Only keep measures categorized as health outcomes and health risk behaviors
        county_df = county_df[county_df['category'].isin(['Health Outcomes', 'Health Risk Behaviors'])]
        county_df = county_df.reset_index(drop=True)
        return county_df
