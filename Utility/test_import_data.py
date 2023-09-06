from collections import Counter
from Utility import Get_dataset_clean_function
import random

# Verification que les valeurs présentes dans le dataset du modele sont bien présentes
# Et que la qualité de certains champs est bien correct

# Verifiaction que les colonnes utilisé pour le dataset sont bien présentes
def test_colonne_in_df():
    df = Get_dataset_clean_function.download_data('https://files.data.gouv.fr/geo-dvf/latest/csv/2022/full.csv.gz')
    columns_use_in_model = ['id_mutation',
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
                            ]
    assert Get_dataset_clean_function.check_columns(df, columns_use_in_model) is True


# Verification que chaque groupe d'identifiant possède bien une seule date et une seule valeur fonciere.
def test_valeur_fonciere_in_df():
    df = Get_dataset_clean_function.download_data('https://files.data.gouv.fr/geo-dvf/latest/csv/2022/full.csv.gz')
    df.dropna(subset=['valeur_fonciere'], inplace=True)
    test_ter = df.loc[:, ['id_mutation', 'valeur_fonciere', 'date_mutation']]
    list_id = list(dict.fromkeys(test_ter .id_mutation.values))
    random.shuffle(list_id)
    for i in list_id[0:10000]:
        assert len(Counter(test_ter.loc[test_ter.id_mutation == i, 'valeur_fonciere'])) == 1
        assert len(Counter(test_ter.loc[test_ter.id_mutation == i, 'date_mutation'])) == 1
