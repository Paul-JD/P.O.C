from collections import Counter

from Utility import Get_dataset_clean_function


# Verification que les valeurs présentes dans le dataset du modele sont bien présentes
# Et que la qualité de certains champs est bien correct

def test_colonne_in_df():
    df = Get_dataset_clean_function.download_data('https://files.data.gouv.fr/geo-dvf/latest/csv/2018/full.csv.gz')
    columns_use_in_model = ['id_mutation',
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
                            ]
    assert Get_dataset_clean_function.check_columns(df, columns_use_in_model) is True


# Verify every id as one 'valeur_fonciere' and one 'date_ime'
def test_valeur_fonciere_in_df():
    df = Get_dataset_clean_function.download_data('https://files.data.gouv.fr/geo-dvf/latest/csv/2018/full.csv.gz')
    df.dropna(subset=['valeur_fonciere'], inplace=True)
    test_ter = df.loc[0:1000, ['id_mutation', 'valeur_fonciere', 'date_mutation']]
    list_id = list(dict.fromkeys(test_ter .id_mutation.values))
    for i in list_id:
        assert len(Counter(test_ter.loc[test_ter.id_mutation == i, 'valeur_fonciere'])) == 1
        assert len(Counter(test_ter.loc[test_ter.id_mutation == i, 'date_mutation'])) == 1
