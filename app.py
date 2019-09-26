# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify , send_file, render_template
import flask_excel as excel
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
import pprint
from bson.json_util import dumps
import json
import datetime
from io import BytesIO
import pandas as pd
import os

class JSONEncoder(json.JSONEncoder):
    ''' extend json-encoder class'''

    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)


app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/semaforo"
mongo = PyMongo(app)

app.json_encoder = JSONEncoder
@app.route('/')
def hello():
    return "Hello World!"

@app.route('/ola/')
@app.route('/ola/<name>')
def ola(name=None):
    return render_template('ola.html', name=name)

@app.route("/download", methods=['GET'])
def download_file():
    cursor   = mongo.db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    #return render_template("index.html",online_users=online_users)
    pprint.pprint(cursor)
    rows = ['pedro' , 'marta' , 'rodrigo']
    rows1 = [ ['Pedro'] , ['Marta'] , ['Rodrigo']]
    return excel.make_response_from_tables(mongo.db.sesseion, [cursor], "xls")
    #return excel.make_response_from_array([ [1, 2], [3, 4] ], "csv", file_name="export_data")

@app.route("/excel", methods=['GET'])
def down_excel():
    items   = mongo.db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    output = BytesIO()
    df = pd.DataFrame.from_records( items )
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, 'data', index=False)
    writer.save()
    output.seek(0)
    return send_file(output, attachment_filename='lista.xlsx', as_attachment=True)


@app.route("/excel1", methods=['GET'])
def pd_excel():
    output = BytesIO()
    # Create a Pandas dataframe from the data.
    df = pd.DataFrame({'Data': [10, 20, 30, 20, 15, 30, 45]})
    writer = pd.ExcelWriter(output, engine='xlsxwriter',  
                                    date_format='YYYY-MM-DD',
                                    datetime_format='YYYY-MM-DD HH:MM:SS')

    df.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    output.seek(0)

    return send_file(output, attachment_filename='simple.xlsx', as_attachment=True)

@app.route("/excel/import", methods=['GET'])
def import_content():
    filepath = 'estados.csv'
    #items   = mongo.db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    #mng_client = pymongo.MongoClient('localhost', 27017)
    #mng_db = mng_client['mongodb_name'] # Replace mongo db name
    #collection_name = 'collection_name' # Replace mongo db collection name
    #db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)

    data = pd.read_csv(file_res)
    data_json = json.loads(data.to_json(orient='records'))
    #db_cm.remove()
    data = mongo.db.estados.insert(data_json)
    
    resp = jsonify({
        'status': 200,
        'rows': data,
        'message': "OK"
    })

    resp.status_code = 200

    return resp

@app.route("/custom_export", methods=['GET'])
def docustomexport():
    query_sets = mongo.db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    column_names = ['id', 'ts']
    return excel.make_response_from_array(list(query_sets), column_names, "xls")

@app.route('/telemetry', methods=['GET'])
def user():
    data = mongo.db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    
    resp = jsonify({
        'status': 200,
        'rows': data,
        'message': "OK"
    })

    resp.status_code = 200

    return resp


@app.route('/telemetry2', methods=['GET'])
def telemetria():
    data = mongo.db.semaphore_telemetry.find({'id': '0001A12X1201804'})
    telemetry = []
    for j in data:
        j.pop('_id')
        j.pop('data')
        telemetry.append(j)
    return jsonify(telemetry)

@app.route("/telemetria/<_id>", methods=['GET'])
def show_post(_id):
   
    # NOTE!: converting _id from string to ObjectId before passing to find_one
    #telemetria = mongo.db.semaphore_telemetry.find_one({'_id': ObjectId(_id)})
    telemetria = mongo.db.semaphore_telemetry.find({'id': _id})
    return render_template('telemetria.html', telemetria=telemetria)





@app.route('/api/<name>', methods=['GET'])
def teste_2014(name): 
    """ http://127.0.0.1:5000/api/axel """
    resp = jsonify({
        'status': 201,
        'rows': name,
        'message': "OK"
    })

    resp.status_code = 201
    return resp

@app.route('/query', methods=['GET'])
def teste_query_string(): 
    """ http://127.0.0.1:5000/query?page=10&filter=test """
    page = request.args.get('page', default = 1, type = int)
    
    filtro = request.args.get('filter', default = '*', type = str)
    
    resp = jsonify({
        'status': 403,
        'page': page,
        'filtro': filtro,
        'message': "OK"
    })

    resp.status_code = 403
    return resp




@app.errorhandler(404)
def page_not_found(e):
    """Send message to the user with notFound 404 status."""
    # Message to the user
    message = {
        "err":
            {
                "msg": "This route is currently not supported. Please refer API documentation."
            }
    }
    # Making the message looks good
    resp = jsonify(message)
    # Sending OK response
    resp.status_code = 404
    # Returning the object
    return resp


if __name__ == '__main__':
    excel.init_excel(app)
    app.run(debug=True)
    #app.run()