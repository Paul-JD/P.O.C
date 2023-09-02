import pandas as pd
from pandas import DataFrame

from Utility.Get_dataset_clean_function import check_columns, download_data, create_street_codex


# Test file of the data import from the url link
#
# The goal here is to check if there has not been changes in the data set
# who could alter the treatment of our model

def test_check_columns():
    df1 = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
    df2 = pd.DataFrame(data={'col1': [1, 2], 'col3': [3, 4]})
    df3 = pd.DataFrame(data={'col1': [1, 2], 'col3': [3, 4]})
    columns_name = ['col1', 'col2']
    assert check_columns(df1, columns_name) is True
    assert check_columns(df2, columns_name) is False
    assert check_columns(df3, columns_name) is False


def test_download_data():
    url1 = "http://nimportquoi.hh"
    url2 = "https://files.data.gouv.fr/geo-dvf/latest/csv/2018/full.csv.gz"
    url3 = '../Data_Files/ABREVIATION_VOIE.csv'
    url4 = 'test/importe_quoi.csv'

    # WEB url
    assert download_data(url1) is None
    assert type(download_data(url2)) == DataFrame

    # Filepath
    assert download_data(url4) is None
    assert type(download_data(url3)) == DataFrame


def test_street_codex():
    street_short = download_data('../Data_Files/ABREVIATION_VOIE.csv').values
    street_list_1 = ['RUE SACLAY', 'AV FOCH', '', 'LA HAVANE', 'SQ DU HAMEAU']
    street_list_2 = []
    result_list = ['RUE', 'AV', 'OTHER', 'OTHER', 'SQ']

    assert result_list == create_street_codex(street_list_1, street_short)
    assert [] == create_street_codex(street_list_2, street_short)


def test_import_data_in_blob():
    pass
