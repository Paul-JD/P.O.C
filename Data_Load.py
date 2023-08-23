import pandas as pd
import io
import requests

url = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20230405-154703/valeursfoncieres-2018.txt"
s = requests.get(url).content
c = pd.read_csv(io.StringIO(s.decode('utf-8')), sep='|')
# c = pd.read_csv(filepath_or_buffer="Current_Data_File/understand.csv", sep=",")

c = c.drop(columns=['Identifiant de document',
                    'Reference document',
                    '1 Articles CGI',
                    '2 Articles CGI',
                    '3 Articles CGI',
                    '4 Articles CGI',
                    '5 Articles CGI',
                    'Code commune',
                    'Prefixe de section',
                    'Section',
                    'No plan',
                    'No Volume',
                    '1er lot',
                    'Surface Carrez du 1er lot',
                    '2eme lot',
                    'Surface Carrez du 2eme lot',
                    '3eme lot',
                    'Surface Carrez du 3eme lot',
                    '4eme lot',
                    'Surface Carrez du 4eme lot',
                    '5eme lot',
                    'Surface Carrez du 5eme lot',
                    'Identifiant local'
                    ], axis=1)

print('ok')
