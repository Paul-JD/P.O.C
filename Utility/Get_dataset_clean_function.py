import gzip
import io

import numpy as np
import pandas as pd
import requests
from azure.storage.blob import BlobClient
from pandas import DataFrame

from Utility import MyThread


def download_data(url: str) -> DataFrame:
    if url.startswith('http://') or url.startswith('https://'):
        try:
            req = requests.get(url)
            df = pd.read_csv(io.BytesIO(gzip.decompress(req.content)))
            return df
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)
    else:
        try:
            df = pd.read_csv(filepath_or_buffer=url, sep=',')
            return df
        except FileNotFoundError:
            print('Bad location')


def check_columns(df: DataFrame, columns: list) -> bool:
    pending_columns = df.columns
    for col in columns:
        if col not in pending_columns:
            return False
    return True


def cleaning_before_threading(dataset) -> DataFrame:
    # Passage de la colonnes date mutation en datetime puis en seconde pour avoir des int
    dataset.loc[:, 'date_mutation'] = pd.to_datetime(dataset['date_mutation'], format='mixed')
    dataset.loc[:, 'date_mutation'] = dataset['date_mutation'].apply(lambda x: x.timestamp())
    # Remplissage des valeurs NaN nature_mutation et labelisation
    dataset.loc[:, 'nature_mutation'] = dataset.nature_mutation.fillna("")
    dataset.loc[:, 'adresse_nom_voie'] = dataset.adresse_nom_voie.fillna("")
    abrv_voie = download_data_from_blob('ABREVIATION_VOIE.csv').values
    codex_voie = create_street_codex(dataset.adresse_nom_voie.values, abrv_voie)

    dataset.loc[:, 'prefixe_voie'] = codex_voie
    dataset.loc[:, 'code_postal'] = dataset.code_postal.fillna(0)
    dataset.loc[:, 'surface_reelle_bati'] = dataset.surface_reelle_bati.fillna(0)
    dataset.loc[:, 'surface_terrain'] = dataset.surface_terrain.fillna(0)
    dataset.loc[:, 'type_local'] = dataset.type_local.fillna('Autre')
    dataset.loc[:, 'nombre_pieces_principales'] = dataset.nombre_pieces_principales.fillna(0)
    dataset.loc[:, 'code_nature_culture'] = dataset.code_nature_culture.fillna("")
    dataset.loc[:, 'code_nature_culture_speciale'] = dataset.code_nature_culture_speciale.fillna("")

    return dataset


def create_street_codex(street_list, street_short) -> list:
    codex_street = list()

    for i in street_list:
        list_nom_rue = i.split(' ')
        first_value = list_nom_rue[0]

        if first_value in street_short:
            codex_street.append(first_value)
        else:
            codex_street.append('OTHER')
    return codex_street


def data_for_model(data: DataFrame, model_data_columns) -> DataFrame:
    list_id = list(dict.fromkeys(data.id_mutation.values))
    thread_list = list(range(len(list_id)))

    for i, id_mutation in enumerate(list_id):
        tmp = data.loc[data.id_mutation == id_mutation, :]
        thread_list[i] = MyThread.MyThread(tmp, model_data_columns)
        thread_list[i].start()
        data = data.drop(data[data.id_mutation == id_mutation].index)

    list_pandas = []
    for j in range(len(list_id)):
        thread_list[j].join()
        list_pandas.append(thread_list[j].retour)

    return pd.concat(list_pandas, ignore_index=True, axis=0).fillna(0)


def import_data_in_blob(data: DataFrame, year: int) -> None:
    connection_string = ('DefaultEndpointsProtocol=https;AccountName=pauljrd;AccountKey=j3Cii5z6+5TDrvCTqnJ74'
                         '+itjPAUcVPFHNEYr7Q6Utcb9vV/qy80gfv7RCnck94MSWJhjxeSKCGL+ASt0csQyQ==;EndpointSuffix=core'
                         '.windows.net')

    storage_account_name = 'pauljrd'
    container_name = 'filestorage'

    blob_client = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name='data_for_model_' + str(year) + '.csv')

    file = data.to_csv(encoding='utf-8')
    filename = 'Data_for_model_' + str(year) + '.csv'
    blob_client.upload_blob(file, overwrite=True)


def download_data_from_blob(blob_name: str) -> DataFrame:
    connection_string = ('DefaultEndpointsProtocol=https;AccountName=pauljrd;AccountKey=j3Cii5z6+5TDrvCTqnJ74'
                         '+itjPAUcVPFHNEYr7Q6Utcb9vV/qy80gfv7RCnck94MSWJhjxeSKCGL+ASt0csQyQ==;EndpointSuffix=core'
                         '.windows.net')
    container_name = 'filestorage'

    blob_client = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name=blob_name)

    # encoding param is necessary for readall() to return str, otherwise it returns bytes
    downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
    blob_text = downloader.readall()
    df = pd.read_csv(io.StringIO(blob_text), sep=',')
    return df


def main_cleaning(year, departement:list) -> None:
    url = 'https://files.data.gouv.fr/geo-dvf/latest/csv/'+str(year)+'/full.csv.gz'
    df = download_data(url)

    # suppression valeur fonciere nulle
    df.dropna(subset=['valeur_fonciere'], inplace=True)
    df.dropna(subset=['longitude', 'latitude'], inplace=True)
    df.drop(df[df['valeur_fonciere']<100000].index, inplace=True)
    df = df.loc[df.loc[df['code_departement'].isin(departement  )].index, :]
    # Selection des colonnes utiles
    dataset = df[['id_mutation',
                  'date_mutation',
                  'nature_mutation',
                  'adresse_nom_voie',
                  'code_postal',
                  'code_commune',
                  'code_departement',
                  'nombre_lots',
                  'type_local',
                  'surface_reelle_bati',
                  'nombre_pieces_principales',
                  'code_nature_culture',
                  'code_nature_culture_speciale',
                  'surface_terrain',
                  'valeur_fonciere',
                  'longitude',
                  'latitude'
                  ]]

    dataset = cleaning_before_threading(dataset)

    # Recuperation données pour construction modele ml

    classe_liste_nature_mutation = list(dict.fromkeys(dataset.nature_mutation.to_list()))
    classe_liste_code_type_local = list(dict.fromkeys(dataset.type_local.to_list()))
    classe_liste_prefixe_voie = list(dict.fromkeys(dataset.prefixe_voie))
    classe_liste_code_culture = download_data_from_blob('CODE_CULTURE.csv')
    classe_liste_code_culture = list(classe_liste_code_culture['Code_nature_culture'].to_dict().values())
    classe_liste_code_culture_spe = download_data_from_blob('CODE_CULTURE_SPECIALE.csv')
    classe_liste_code_culture_spe = list(classe_liste_code_culture_spe['CODE_CULTURE_SPECIALE'].to_dict().values())

    nom_colonnes_from_dataset = [
        'date_mutation',
        'code_postal',
        'code_commune',
        'code_departement',
        'nombre_lots',
        'surface_reelle_bati',
        'nombre_pieces_principales',
        'surface_terrain',
        'valeur_fonciere',
        'longitude',
        'latitude'
    ]

    colonnes = (nom_colonnes_from_dataset
                + classe_liste_code_type_local
                + classe_liste_prefixe_voie
                + classe_liste_nature_mutation
                + classe_liste_code_culture
                + classe_liste_code_culture_spe)

    colonnes = np.array(colonnes)
    del classe_liste_nature_mutation, (
        classe_liste_code_type_local), (
        classe_liste_prefixe_voie), (
        classe_liste_code_culture), (
        classe_liste_code_culture_spe)

    dataset_for_model = data_for_model(dataset, colonnes)

    import_data_in_blob(dataset_for_model, year)
