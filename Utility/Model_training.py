import pickle
from math import log

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split

from Utility.Get_dataset_clean_function import download_data_from_blob
from Utility.Model_training_function import get_confusion_matrix

df = download_data_from_blob('data_for_model_2022.csv')
df.drop(labels=['Unnamed: 0'], inplace=True, axis = 1)
df['log_val'] = df.loc[:, 'valeur_fonciere'].apply(lambda x: log(x))

desc = df.describe()
df.loc[df['log_val'] <= desc.loc['25%', 'log_val'], 'log_class'] = 0
df.loc[(df['log_val'] > desc.loc['25%', 'log_val']) & (df['log_val'] <= desc.loc['50%', 'log_val']), 'log_class'] = 1
df.loc[(df['log_val'] > desc.loc['50%', 'log_val']) & (df['log_val'] <= desc.loc['75%', 'log_val']), 'log_class'] = 2
df.loc[(df['log_val'] > desc.loc['75%', 'log_val']), 'log_class'] = 3

X = df.drop(labels=['valeur_fonciere', 'log_val', 'log_class'], axis=1)
Y = df['log_class']

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, train_size=0.7, random_state=42)

forest = RandomForestClassifier(n_estimators=1000,  # Number of trees
                                max_features=2,  # Num features considered
                                bootstrap=False)
forest.fit(X_train, Y_train)

Y_pred = forest.predict(X_test)
get_confusion_matrix(Y_pred, Y_test)

outputfile = f'model_C.bin'

with open(outputfile, 'wb') as f_out:
    pickle.dump(forest, f_out)

X_test.reset_index(drop=True).loc[0,:].to_json(r'test.json')

print(forest.predict(X_test.reset_index(drop=True).loc[0,:]))