import pickle
from math import log

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split

from Utility.Get_dataset_clean_function import download_data_from_blob, upload_data_in_blob


def get_confusion_matrix(y_test, y_pred):
    cm = confusion_matrix(y_test, y_pred)
    print(cm)
    print(classification_report(y_test, y_pred))


def model_training() -> None:
    # telechargement du dataset
    df = download_data_from_blob('data_for_model_2022.csv', 'filestorage')

    # index passe en colonnes sont retiré
    df.drop(labels=['Unnamed: 0'], inplace=True, axis=1)

    # log la valeur fonciere pour avoir un ecart type moins grand
    df['log_val'] = df.loc[:, 'valeur_fonciere'].apply(lambda lbd: log(lbd))

    # Sauvegarde la distribution des données et les classes en 4 classes 0-25% , 25-50%, 50-75% et 75-100%.
    desc = df.describe()
    premier_quartier = desc.loc['25%', 'log_val']
    deuxieme_quartier = desc.loc['50%', 'log_val']
    troisieme_quartier = desc.loc['75%', 'log_val']

    # definition des classes
    df.loc[df['log_val'] <= premier_quartier, 'log_class'] = 0
    df.loc[(df['log_val'] > premier_quartier) & (df['log_val'] <= deuxieme_quartier), 'log_class'] = 1
    df.loc[(df['log_val'] > deuxieme_quartier) & (df['log_val'] <= troisieme_quartier), 'log_class'] = 2
    df.loc[(df['log_val'] > troisieme_quartier), 'log_class'] = 3

    x = df.drop(labels=['valeur_fonciere', 'log_val', 'log_class'], axis=1)
    y = df['log_class']

    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.7, random_state=42)

    forest = RandomForestClassifier(n_estimators=1000,  # Number of trees
                                    max_features=2,  # Num features considered
                                    bootstrap=False)
    forest.fit(x_train, y_train)

    y_pred = forest.predict(x_test)
    get_confusion_matrix(y_pred, y_test)


    test = pickle.dumps(forest)
    upload_data_in_blob(test, 2022, 'modelstorage')

    #output = 'model_C.bin'
    #with open(output,'wb') as f_out:
    #    pickle.dump(forest,f_out)
