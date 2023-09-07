import gzip
import io

import numpy as np
import pandas as pd
import requests
from azure.storage.blob import BlobClient
from pandas import DataFrame

from Utility import MyThread


# Telecharger les données depuis un fichier local ou distant
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


# Verifier que les colonnes dont le modele a besoin sont bien présentes
def check_columns(df: DataFrame, columns: list) -> bool:
    pending_columns = df.columns
    for col in columns:
        if col not in pending_columns:
            return False
    return True


# Action de nettoyage sur les différentes colonnes du dataset
def cleaning_before_threading(dataset) -> DataFrame:
    # Passage de la colonnes date mutation en datetime puis en seconde pour avoir des int
    dataset.loc[:, 'date_mutation'] = pd.to_datetime(dataset['date_mutation'], format='mixed')
    dataset.loc[:, 'date_mutation'] = dataset['date_mutation'].apply(lambda x: x.timestamp())

    # Remplissage des valeurs NaN nature_mutation et labelisation
    dataset.loc[:, 'nature_mutation'] = dataset.nature_mutation.fillna("")
    dataset.loc[:, 'adresse_nom_voie'] = dataset.adresse_nom_voie.fillna("")

    # Validation prefixe des VOIES
    abrv_voie = download_data_from_blob('ABREVIATION_VOIE.csv', 'filestorage').values
    codex_voie = create_street_codex(dataset.adresse_nom_voie.values, abrv_voie)
    dataset.loc[:, 'prefixe_voie'] = codex_voie

    # Remplissage des valeurs Nan des colonnes
    dataset.loc[:, 'code_postal'] = dataset.code_postal.fillna(0)
    dataset.loc[:, 'surface_reelle_bati'] = dataset.surface_reelle_bati.fillna(0)
    dataset.loc[:, 'surface_terrain'] = dataset.surface_terrain.fillna(0)
    dataset.loc[:, 'type_local'] = dataset.type_local.fillna('Autre')
    dataset.loc[:, 'nombre_pieces_principales'] = dataset.nombre_pieces_principales.fillna(0)
    dataset.loc[:, 'lot1_surface_carrez'] = dataset.lot1_surface_carrez.fillna(0)
    dataset.loc[:, 'lot2_surface_carrez'] = dataset.lot2_surface_carrez.fillna(0)
    dataset.loc[:, 'lot3_surface_carrez'] = dataset.lot3_surface_carrez.fillna(0)
    dataset.loc[:, 'lot4_surface_carrez'] = dataset.lot4_surface_carrez.fillna(0)

    return dataset


# Recuperation des prefixes de voie
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


# Execution des threads creeant le dataset passé au modele
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
        if thread_list[j].retour is not None:
            print(1)
            list_pandas.append(thread_list[j].retour)
    return pd.concat(list_pandas, ignore_index=True, axis=0).fillna(0)


# Upload du dataset pour le modele depuis un container Azure Blob, en spécifiant l'annee.
def upload_data_in_blob(data, year: int, container_name) -> None:
    connection_string = ('DefaultEndpointsProtocol=https;AccountName=pauljrd;AccountKey=j3Cii5z6+5TDrvCTqnJ74'
                         '+itjPAUcVPFHNEYr7Q6Utcb9vV/qy80gfv7RCnck94MSWJhjxeSKCGL+ASt0csQyQ==;EndpointSuffix=core'
                         '.windows.net')

    if type(data) == DataFrame:
        filename = 'Data_for_model_' + str(year) + '.csv'
        blob_client = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
            blob_name=filename)

        file = data.to_csv(encoding='utf-8')

        blob_client.upload_blob(file, overwrite=True)
    else:
        blob_client = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
            blob_name='model_C.bin')

        blob_client.upload_blob(data, overwrite=True)


# Telechargement d'un container Blob.
def download_data_from_blob(blob_name: str, container_name: str):
    # lien vers Azure file Storage
    connection_string = ('DefaultEndpointsProtocol=https;AccountName=pauljrd;AccountKey=j3Cii5z6+5TDrvCTqnJ74'
                         '+itjPAUcVPFHNEYr7Q6Utcb9vV/qy80gfv7RCnck94MSWJhjxeSKCGL+ASt0csQyQ==;EndpointSuffix=core'
                         '.windows.net')

    if container_name == 'filestorage':
        # Connection au blob
        blob_client = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
            blob_name=blob_name)

        # recuperation et conversion en dataframe
        downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
        blob_text = downloader.readall()
        df = pd.read_csv(io.StringIO(blob_text), sep=',')
        return df
    else:
        # Connection au blob
        blob_client = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
            blob_name=blob_name)

        # recuperation et conversion en dataframe
        downloader = blob_client.download_blob(max_concurrency=1)
        return downloader.readall()


def main_cleaning(year, departement: list) -> None:
    url = 'https://files.data.gouv.fr/geo-dvf/latest/csv/' + str(year) + '/full.csv.gz'
    df = download_data(url)

    # suppression valeur fonciere nulle
    df.dropna(subset=['valeur_fonciere'], inplace=True)

    # suppression des doublons
    df.drop_duplicates(inplace=True)

    # suppression des emplacements geolocalisations
    df.dropna(subset=['longitude', 'latitude'], inplace=True)

    # suppression des valeurs foncieres inférieur à 100 000 Euros
    df.drop(df[df['valeur_fonciere'] < 100000].index, inplace=True)

    # selection du departement
    df = df.loc[df.loc[df['code_departement'].isin(departement)].index, :]

    # selection maison et appartement
    df = df.loc[df.loc[df['code_type_local'].isin([1, 2])].index, :]
    df.reset_index(drop=True, inplace=True)

    # Selection des colonnes utiles
    dataset = df[['id_mutation',
                  'date_mutation',
                  'nature_mutation',
                  'adresse_nom_voie',
                  'code_postal',
                  'nombre_lots',
                  'lot1_surface_carrez',
                  'lot2_surface_carrez',
                  'lot3_surface_carrez',
                  'lot4_surface_carrez',
                  'type_local',
                  'surface_reelle_bati',
                  'nombre_pieces_principales',
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

    nom_colonnes_from_dataset = [
        'date_mutation',
        'code_postal',
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
                + classe_liste_nature_mutation)

    colonnes = np.array(colonnes)

    # suppression de variable prenant de la place
    del classe_liste_nature_mutation, (
        classe_liste_code_type_local), (
        classe_liste_prefixe_voie), (
        df
    )

    dataset_for_model = data_for_model(dataset, colonnes)
    upload_data_in_blob(dataset_for_model, year, 'filestorage')
