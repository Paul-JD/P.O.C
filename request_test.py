import pandas as pd
import requests
import jsonify

url = 'http://localhost:9696/predict'

appartement ={
  "ALL": 0.0,
  "AV": 0.0,
  "Adjudication": 0.0,
  "Appartement": 1.0,
  "BD": 0.0,
  "CITE": 0.0,
  "CR": 0.0,
  "CRS": 0.0,
  "Echange": 0.0,
  "IMP": 0.0,
  "Maison": 0.0,
  "OTHER": 0.0,
  "PL": 0.0,
  "QUAI": 0.0,
  "RLE": 0.0,
  "RUE": 1.0,
  "SQ": 0.0,
  "VC": 0.0,
  "VOIE": 0.0,
  "Vente": 1.0,
  "Vente en l'\u00e9tat futur d'ach\u00e8vement": 0.0,
  "Vente terrain \u00e0 b\u00e2tir": 0.0,
  "code_postal": 75007.0,
  "date_mutation": 1656547200.0,
  "latitude": 48.859696,
  "longitude": 2.324686,
  "nombre_lots": 4.0,
  "nombre_pieces_principales": 6.0,
  "surface_reelle_bati": 183.0,
  "surface_terrain": 0.0
}

response = requests.post(url, json=appartement)

print(response)