import gzip
import io
import time

import numpy as np
import pandas as pd
import requests

from Utility.MyThread import MyThread

url = "https://files.data.gouv.fr/geo-dvf/latest/csv/2018/full.csv.gz"
req = requests.get(url)
df = pd.read_csv(io.BytesIO(gzip.decompress(req.content)))

# suppression valeur fonciere nulle
df.dropna(subset=['valeur_fonciere'], inplace=True)
df.dropna(subset=['longitude', 'latitude'], inplace=True)

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

# Passage de la colonnes date mutation en datetime puis en seconde pour avoir des int
dataset.loc[:, 'date_mutation'] = pd.to_datetime(dataset['date_mutation'], format='mixed')
dataset.loc[:, 'date_mutation'] = dataset['date_mutation'].apply(lambda x: x.timestamp())
# Remplissage des valeurs NaN nature_mutation et labelisation
dataset.loc[:, 'nature_mutation'] = dataset.nature_mutation.fillna("")

abrv_voie = pd.read_csv(filepath_or_buffer='Data_Files/ABREVIATION_VOIE.csv', sep=',').values

dataset.loc[:, 'adresse_nom_voie'] = dataset.adresse_nom_voie.fillna("")

codex_voie = list()
for i in dataset.adresse_nom_voie.values:

    list_nom_rue = i.split(' ')
    premier_valeur_liste = list_nom_rue[0]

    if premier_valeur_liste in abrv_voie:
        codex_voie.append(premier_valeur_liste)
    else:
        codex_voie.append('AUTRE')

dataset.loc[:, 'prefixe_voie'] = codex_voie
dataset.loc[:, 'code_postal'] = dataset.code_postal.fillna(0)
dataset.loc[:, 'surface_reelle_bati'] = dataset.surface_reelle_bati.fillna(0)
dataset.loc[:, 'surface_terrain'] = dataset.surface_terrain.fillna(0)
dataset.loc[:, 'type_local'] = dataset.type_local.fillna('Autre')
dataset.loc[:, 'nombre_pieces_principales'] = dataset.nombre_pieces_principales.fillna(0)
dataset.loc[:, 'code_nature_culture'] = dataset.code_nature_culture.fillna("")
dataset.loc[:, 'code_nature_culture_speciale'] = dataset.code_nature_culture_speciale.fillna("")

classe_liste_nature_mutation = list(dict.fromkeys(list(dataset.nature_mutation.values)))
classe_liste_code_type_local = list(dict.fromkeys(list(dataset.type_local.values)))
classe_liste_prefixe_voie = list(dict.fromkeys(codex_voie))
classe_liste_code_culture = pd.read_csv(filepath_or_buffer='Data_Files/CODE_CULTURE.csv', sep=',')
classe_liste_code_culture = list(classe_liste_code_culture['Code_nature_culture'].to_dict().values())
classe_liste_code_culture_spe = pd.read_csv(filepath_or_buffer='Data_Files/CODE_CULTURE_SPECIALE.csv', sep=',')
classe_liste_code_culture_spe = list(classe_liste_code_culture_spe['CODE_CULTURE_SPECIALE'].to_dict().values())
Nom_colonnes_from_dataset = [
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

colonnes = (Nom_colonnes_from_dataset
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

test = dataset.loc[0:10]

list_id_mutation = list(dict.fromkeys(test.id_mutation.values))

th_liste: list[range | MyThread] = list(range(len(list_id_mutation)))
Data = pd.DataFrame()

t1 = time.time()
for i, id in enumerate(list_id_mutation):
    tmp = test.loc[test.id_mutation == id, :]
    th_liste[i]: MyThread = MyThread(tmp, colonnes)
    th_liste[i].start()
    test = test.drop(test[test.id_mutation == id].index)

list_pandas = []
for j in range(len(list_id_mutation)):
    th_liste[j].join()
    list_pandas.append(th_liste[j].retour)
Data = pd.concat(list_pandas, ignore_index=True, axis=0)

Data = Data.fillna(0)
print(time.time()-t1)

Data.to_csv(path_or_buf='C:/Users/dargo/Files_clean/2018_data_set_clean.csv', sep=',', index=False)
