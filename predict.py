import pickle

import pandas as pd
from flask import Flask, request, jsonify

with open('Utility/model_C.bin', 'rb') as f_in:
    model = pickle.load(f_in)

app = Flask('Appartement')


@app.route('/predict', methods=['POST'])
def predict():

    X = request.get_json()
    appartement = pd.DataFrame(X,index=[0])
    result = model.predict(appartement)[0]
    retour = {
        'prediction': result
    }
    return jsonify(retour)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9696)
