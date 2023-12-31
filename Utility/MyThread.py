from collections import Counter
from threading import Thread

import pandas as pd
from pandas import DataFrame


# Thread executant la transformation du dataset
class MyThread(Thread):
    def __init__(self, dataset: DataFrame, colonnes: list):
        super(MyThread, self).__init__()
        self.dataset = dataset
        self.colonnes = colonnes
        self.retour = pd.DataFrame()

    def run(self) -> None:

        # recuperation du dataset
        tmp_dataset = self.dataset

        # recuperation du 1er index
        index = tmp_dataset.index[0]

        # les données dont stockées dans un dictionnaire ayant pour clef les différentes colonnes.
        tmp_dict = dict.fromkeys(self.colonnes)

        # allocation des valeurs
        tmp_dict['date_mutation'] = [tmp_dataset.loc[index, 'date_mutation']]
        tmp_dict['code_postal'] = [tmp_dataset.loc[index, 'code_postal']]
        tmp_dict['nombre_lots'] = [tmp_dataset.loc[index, 'nombre_lots'].sum()]

        # somme de la surface bati
        surface_bati = tmp_dataset.loc[index, 'surface_reelle_bati'].sum()

        # sommme de la surface du terrain
        surface_terrain = tmp_dataset.loc[index, 'surface_terrain'].sum()

        # somme des différentes pièces si plusieurs lots.
        tmp_dict['nombre_pieces_principales'] = [tmp_dataset.loc[index, 'nombre_pieces_principales'].sum()]

        # somme des surfaces carrez en fonction des différents lots
        surface_carrez = tmp_dataset.loc[
            index, ['lot1_surface_carrez', 'lot2_surface_carrez', 'lot3_surface_carrez',
                    'lot4_surface_carrez']].sum().sum()

        # allocation surface carrez ou bati
        # si 2 nulles alors reference defini comme de mauvaise qualité
        if surface_carrez != 0:
            tmp_dict['surface_reelle_bati'] = [surface_carrez]
        elif surface_bati != 0:
            tmp_dict['surface_reelle_bati'] = [surface_bati]
        else:
            self.retour = None
            return None

        # valeurs fonciere
        tmp_dict['valeur_fonciere'] = [tmp_dataset.loc[index, 'valeur_fonciere']]

        # coordonnées cardinals
        tmp_dict['longitude'] = [tmp_dataset.loc[index, 'longitude']]
        tmp_dict['latitude'] = [tmp_dataset.loc[index, 'latitude']]

        # valeur classe code type local
        count_type_local = Counter(tmp_dataset.loc[:, 'type_local'])
        for type_local in count_type_local:
            tmp_dict[type_local] = count_type_local[type_local]

        # valeur prefixe voie
        count_prefixe_voie = Counter(tmp_dataset.loc[:, 'prefixe_voie'])
        for abbrev_voie in count_prefixe_voie:
            tmp_dict[abbrev_voie] = count_prefixe_voie[abbrev_voie]

        # valeur classe nature mutation
        count_nature_mutation = Counter(tmp_dataset.loc[:, 'nature_mutation'])
        for nat_mutation in count_nature_mutation:
            tmp_dict[nat_mutation] = count_nature_mutation[nat_mutation]

        self.retour = pd.DataFrame.from_dict(tmp_dict)
