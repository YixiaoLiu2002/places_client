import requests
import pandas as pd
from typing import Dict

class PlacesClient:
    def __init__(self, token):
        """
        Initialize a client object
        """
        self.base_url = 'https://data.cdc.gov/api/v3/views/'
        self.session = requests.Session()
        self.session.headers.update({
            'X-App-Token': token
        })

    def _make_request(self, url: str, params=None):
        """
        Make a get request to the API and return responses in JSON
        """
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"API Error: {e}")
    
    def _json_to_df(self, data) -> pd.DataFrame:
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
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def get_measure_list(self) -> pd.DataFrame:
        """
        Display all Health Outcomes and Health Risk Behaviors Measures 

        Returns
        -------
        measures_df : pandas Data Frame
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
    
    def get_county_data(self, release: str ='2025') -> pd.DataFrame:
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
        if release not in release_ids:
            raise ValueError("This release version is not supported.")

        url = self.base_url + release_ids[release] + '/query.json'

        data = self._make_request(url)
        county_df = self._json_to_df(data)
        
        # Only keep measures categorized as health outcomes and health risk behaviors
        county_df = county_df[county_df['categoryid'].isin(['HLTHOUT', 'RISKBEH'])]
        county_df = county_df.reset_index(drop=True)

        # Drop rows missing the key data
        county_df = county_df.dropna(subset=["data_value"]).reset_index(drop=True)
        return county_df

    def filter_by_measures(self, df: pd.DataFrame, measures: str=None, categories: str=None) -> pd.DataFrame:
        """
        Get a subset of a PLACES DataFrame by measures or categories. 
        Both the short names and ids of measures are supported.
        
        Parameters
        ----------
        df : pandas DataFrame
            The county-level PLACES dataset.
        measures : list of strings
            Short names or measureids of measures to keep.
        categories : list of strings
            Short namse or categoryids of categories to keep.

        Returns
        -------
        sub_df : pandas DataFrame
            A dataframe containing only data of selected measures and/or categories.
        
        Examples
        --------
        >>> new_df = client.filter_by_measures(df, measures=['Physical Inactivity','Current Asthma'])
        >>> new_df = client.filter_by_measures(df, categories=['Health Outcomes'])
        """
        sub_df = df
        if measures:
            sub_df = sub_df[sub_df['short_question_text'].isin(measures) | sub_df['measureid'].isin(measures)]
        if categories:
            sub_df = sub_df[sub_df['category'].isin(categories) | sub_df['categoryid'].isin(categories)]
        return sub_df
    
    def filter_by_regions(self, df: pd.DataFrame, states: str=None, counties: str=None) -> pd.DataFrame:
        """
        Get a subset of a PLACES DataFrame by states or counties. 
        
        Parameters
        ----------
        df : pandas DataFrame
            The county-level PLACES dataset.
        states : list of strings
            Names or stateabbr of states to keep.
        counties : list of strings
            Location ids (FIPS codes) of counties to keep.

        Returns
        -------
        sub_df : pandas DataFrame
            A dataframe containing only data of selected counties and/or states.
        
        Examples
        --------
        >>> new_df = client.filter_by_measures(df, measures=['Physical Inactivity','Current Asthma'])
        >>> new_df = client.filter_by_measures(df, categories=['Health Outcomes'])
        """
        sub_df = df
        if states:
            sub_df = sub_df[sub_df['stateabbr'].isin(states) | sub_df['statedesc'].isin(states)]
        if counties:
            sub_df = sub_df[sub_df['locationid'].isin(counties)]
        return sub_df
    
    def create_pivot_table(self, df: pd.DataFrame, level='county') -> pd.DataFrame:
        """
        Create a wide pivot table that shows all measure values for each county or for each state.
        
        Parameters
        ----------
        df : pandas DataFrame
            The county-level PLACES dataset.
        level : str, optional
            Aggregation level, county or state.

        Returns
        -------
        table : pandas DataFrame
            A pivot table with columns representing measure IDs and rows representing counties or states.
        
        Examples
        --------
        >>> state_table = client.create_pivot_table(df, level='state')
        >>> state_table.head()
        """
        if level not in ['county', 'state']:
            raise ValueError("Level must be 'county' or 'state'.")
    
        # convert df into wide format
        table = df.pivot_table(
            index='locationid',
            columns='measureid',
            values='data_value',
        )

        if level == 'county':
            # add county and state names for county-level table
            id_names = df[['locationid', 'locationname', 'statedesc']].drop_duplicates().set_index('locationid')
            table = table.join(id_names, how='left')
            # move name columns to front
            names = ['locationname', 'statedesc']
            data = [col for col in table.columns if col not in names]
            table = table[names + data]
            return table

        # if level == 'state', aggregate data into state-level
        counties_states = df[['locationid', 'statedesc']].drop_duplicates().set_index('locationid')
        table = table.join(counties_states, how='left')
        table = table.groupby('statedesc').mean(numeric_only=True)
        return table

    def get_correlation(self, df: pd.DataFrame, x:str, y:str) -> Dict[str, float]:
        """
        Calculate the correlation between 2 measures
        
        Parameters
        ----------
        df : pandas DataFrame
            The county-level PLACES dataset.
        x : str
        The measure ID or short name of the first variable.
        y : str
        The measure ID or short name of the second variable.


        Returns
        -------
        result : dict
            A dictionary containing:
            - corr_coef: the correlation coefficient (r)
            - sample_size: number of counties included in calculation
            - mean_x, mean_y: means of measure x and y
        
        Examples
        --------
        >>> client.get_correlation(places_2024, 'LPA', 'DEPRESSION')
        {'corr_coef': 0.20321713670955188, 'sample_size': 1838, 'mean_x': 26.86089867640032, 'mean_y': 23.600384332489686}
        """
        if x is None or y is None:
            raise ValueError("Two measures (x and y) must be provided.")
        if not isinstance(x, str) or not isinstance(y, str):
            raise TypeError("x and y must be strings.")
        measures = df['measureid'].unique()
        if x not in measures or y not in measures:
            raise ValueError("Invalid measureid.")
        
        sub_df = self.filter_by_measures(df, measures=[x, y])

        table = sub_df.pivot_table(values='data_value', index='locationname', columns='measureid')
        table = table.dropna()
        r = table[x].corr(table[y], method='pearson')

        result = {
            'corr_coef': float(r), 
            'sample_size': len(table),
            'mean_x': float(table[x].mean()),
            'mean_y': float(table[y].mean())
        }
        return result
    
    def summarize_measure(self, df: pd.DataFrame, measureid: str) -> Dict[str, float]:
        """
        Offer basic descriptive statistics for a given PLACES measure.

        Parameters
        ----------
        df : pandas DataFrame
            The county-level PLACES dataset.
        measureid : str
            The measure ID of the measure to summarize.

        Returns
        -------
        summary : dict
            Dictionary with mean, median, min, max, and missing value count.
        """
        if measureid not in df['measureid'].unique():
            raise ValueError("Invalid measureid.")

        data = df[df['measureid'] == measureid]['data_value']

        summary_dict =  {
            'mean': float(data.mean()),
            'median': float(data.median()),
            'min': float(data.min()),
            'max': float(data.max()),
            'std': float(data.std()),
            'count': float(data.count()),
            'missing_values_count': float(data.isna().sum())
        }
        return summary_dict
