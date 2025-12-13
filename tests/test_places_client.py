from places_client.places_client import PlacesClient
import pytest
import pandas as pd

@pytest.fixture
def client():
    return PlacesClient("TEST")

def test_get_county_data(mocker, client):
    # mock API JSON response
    mocked_json_data = [
        {
            'year': '2022',
            'stateabbr': 'WI',
            'statedesc': 'Wisconsin',
            'locationname': 'Fond du Lac',
            'category': 'Health Outcomes',
            'measure': 'Stroke among adults',
            'data_value': '3.7',
            'totalpopulation': '103836',
            'locationid': '55039',
            'categoryid': 'HLTHOUT',
            'measureid': 'STROKE',
            'short_question_text': 'Stroke'
        }
    ]

    mocker.patch('places_client.places_client.PlacesClient._make_request', return_value = mocked_json_data)
    df = client.get_county_data('2022')

    assert isinstance(df, pd.DataFrame)
    assert df['measureid'].iloc[0] == 'STROKE'
    assert df['data_value'].iloc[0] == 3.7
    assert df['locationid'].iloc[0] == '55039'

def test_filter_by_measures(client):
    df = pd.DataFrame({
        'measureid': ['CHD', 'DIABETES', 'ARTHRITIS'],
        'short_question_text': ['Coronary Heart Disease', 'diabetes', 'Arthritis'],
        'category': ['Health Outcomes', 'Health Outcomes', 'Health Outcomes'],
        'category_id': ['HLTHOUT', 'HLTHOUT', 'HLTHOUT'],
        'data_value': [10, 20, 30]
    })

    sub_df = client.filter_by_measures(df, measures=['CHD'])
    assert sub_df['measureid'].unique().tolist() == ['CHD']

def test_filter_by_regions(client):
    df = pd.DataFrame({
        'stateabbr': ['CA', 'NY', 'CA'],
        'statedesc': ['California', 'New York', 'California'],
        'locationid': ['10031', '12100', '15440']
    })

    sub_df_state = client.filter_by_regions(df, states=['CA'])
    sub_df_county = client.filter_by_regions(df, counties=['15440'])
    assert sub_df_state['stateabbr'].unique().tolist() == ['CA']
    assert sub_df_county['locationid'].unique().tolist() == ['15440']

def test_create_pivot_table(client):
    df = pd.DataFrame({
        'measureid': ['CHD', 'DIABETES', 'ARTHRITIS'],
        'short_question_text': ['Coronary Heart Disease', 'diabetes', 'Arthritis'],
        'category': ['Health Outcomes', 'Health Outcomes', 'Health Outcomes'],
        'category_id': ['HLTHOUT', 'HLTHOUT', 'HLTHOUT'],
        'data_value': [10, 20, 30],
        'stateabbr': ['CA', 'NY', 'CA'],
        'statedesc': ['California', 'New York', 'California'],
        'locationid': ['10031', '12100', '15440'],
        'locationname': ['aaa', 'bbb', 'ccc']
    })

    table = client.create_pivot_table(df, level='county')

    assert 'CHD' in table.columns
    assert 'DIABETES' in table.columns
    assert table.loc['10031', 'CHD'] == 10

def test_get_correlation(client):
    client = PlacesClient("TEST")

    df = pd.DataFrame({
        'locationname': ['aaa', 'aaa', 'bbb', 'bbb'],
        'measureid': ['X', 'Y', 'X', 'Y'],
        'data_value': [1, 2, 3, 4],
        'short_question_text': ['diabetes', 'Arthritis', 'diabetes', 'Arthritis']
    })

    result = client.get_correlation(df, 'X', 'Y')

    assert 'corr_coef' in result
    assert result['sample_size'] == 2.0
    assert result['mean_x'] == 2.0
    assert result['mean_y'] == 3.0


def test_summarize_measure(client):
    df = pd.DataFrame({
        'measureid': ['X', 'X', 'X'],
        'data_value': [1, 2, 3]
    })

    summary = client.summarize_measure(df, 'X')

    assert summary['mean'] == 2.0
    assert summary['min'] == 1.0
    assert summary['max'] == 3.0
    assert summary['count'] == 3.0