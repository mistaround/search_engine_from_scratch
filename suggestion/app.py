import flask
from flask import Flask, request
from flask_cors import CORS
import json
import main

app = Flask(__name__)
CORS(app)

@app.route('/predict', methods=['get'])
def get_search_suggestion():
    try:
        args = request.args
        input_text = args.get("q")
        input_text += ' <mask>'
        res = main.get_prediction(input_text, topk=5)
        res = app.response_class(response=json.dumps(res), status=200, mimetype='application/json')
        return res
    except Exception as error:
        err = str(error)
        print(err)
        return app.response_class(response=json.dumps(err), status=500, mimetype='application/json')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=8000, use_reloader=False)
