import pickle

import pandas as pd
from flask import Flask, request, jsonify
from Utility.Get_dataset_clean_function import download_data_from_blob

#m = download_data_from_blob('model_C.bin','modelstorage')
#model = pickle.load(m)

with open('./model_C.bin','rb') as f_in:
    model = pickle.load(f_in)

app = Flask('Appartement')


@app.route('/predict', methods=['POST'])
def predict():
    x = request.get_json()
    appartement = pd.DataFrame(x, index=[0])
    result = model.predict(appartement)[0]
    retour = {
        'prediction': result
    }
    return jsonify(retour)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9696)
