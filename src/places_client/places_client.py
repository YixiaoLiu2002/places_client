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
        df = df.drop(
            [':id', ':version', ':created_at', ':updated_at', 'data_value_footnote_symbol', 'data_value_footnote'], 
            axis=1, errors='ignore'
            )
        # convert numeric variables
        numeric_cols = ['data_value', 'low_confidence_limit', 'high_confidence_limit', 'totalpopulation']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col])
        return df

    def get_measure_list(self):
        """
        Retrieve the key information of all available measures 
        (all health outcomes and health risk behaviors measures).

        Returns
        -------
        measures_df: pandas Data Frame
            A dataframe displaying the following the information of filtered measures:
            - id: measure identifier
            - short_name: short label
            - full_name: full descriptive name
            - catgory: measure category (Health Outcomes or Health Risk Behaviors)

        Examples
        --------
        >>> measures = client.get_measure_list()
        >>> measures.head()
        """
        data_dictionary_id = 'm35w-spkz'
        url = self.base_url + data_dictionary_id + '/query.json'

        data = self._make_request(url)
        measures_df = self._json_to_df(data)
        measures_df = measures_df[measures_df['categoryid'].isin(['HLTHOUT', 'RISKBEH'])]
        measures_df = measures_df[['measureid', 'measure_short_name', 'measure_full_name', 'category_name']]
        measures_df.columns = pd.Index(['id', 'short_name', 'full_name', 'category'])
        return measures_df
    
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
        >>> df = client.get_county_data('2023')
        >>> df.head()
        """
        release_ids = {
            '2025': 'swc5-untb',
            '2024': 'fu4u-a9bh',
            '2023': 'h3ej-a9ec',
            '2022': 'duw2-7jbt',
            '2021': 'pqpp-u99h',
            '2020': 'dv4u-3x3q'
        }
        
        if not isinstance(release, str):
            raise TypeError("The release must be a string.")
        elif release not in release_ids:
            raise ValueError("This release version is not supported.")
        else:
            url = self.base_url + release_ids[release] + '/query.json'

        data = self._make_request(url)
        county_df = self._json_to_df(data)
        
        # Filter measures categorized as health outcomes and health risk behaviors
        county_df = county_df[county_df['categoryid'].isin(['HLTHOUT', 'RISKBEH'])]
        county_df = county_df.reset_index(drop=True)
        return county_df

    def filter_by_measures(self, df, measures=None, categories=None, measure_ids=None):
        """
        Get a subset of a PLACES DataFrame by measures or categories. 
        Both the short names and ids of measures are supported.
        
        Parameters
        ----------
        df : pandas DataFrame
            The dataframe to subset from.
        measures: list of strings
            Short names of measures to keep.
        categories: list of strings
            Short names of categories to keep.
        measure_ids: list of strings
            ids of measures to keep.


        Returns
        -------
        sub_df : pandas DataFrame
            A dataframe containing only selected measures and/or categories.
        
        Examples
        --------
        >>> new_df = client.filter_by_measures(df, measures=['Physical Inactivity','Current Asthma'])
        >>> new_df = client.filter_by_measures(df, categories=['Health Outcomes'])
        """
        sub_df = df
        if measures:
            sub_df = sub_df[sub_df['short_question_text'].isin(measures)]
        if categories:
            sub_df = sub_df[sub_df['category'].isin(categories)]
        if measure_ids:
            sub_df = sub_df[sub_df['measureid'].isin(measure_ids)]
        return sub_df
