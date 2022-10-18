from flask import Flask, jsonify, request, render_template, session, redirect, url_for
from flask import Blueprint
import os
import sys
import requests
module_path = os.path.dirname(os.path.abspath(__file__))
if module_path not in sys.path:
    sys.path.append(module_path)

from werkzeug.utils import secure_filename
from os.path import join, dirname, realpath
import json

os.chdir(os.path.dirname(__file__))

app = Flask(__name__)
app.config['DEBUG'] = True

uploads = Blueprint('uploads', __name__)


@uploads.route('/uploads',  methods=("POST", "GET"))
def uploading():
    """ Function for upload a CSV to flask server"""

    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}

    if request.method == 'POST':

        uploaded_file = request.files['uploaded-file']
        datas = json.load(uploaded_file)
        filename = secure_filename(uploaded_file.filename)
        file_location = f"../uploads/{uploaded_file.filename}"
       
        with open(file_location, 'w', encoding='utf-8') as f:
            json.dump(datas, f, ensure_ascii=False, indent=4)

        #print (file_location)
        session['uploaded_file'] = file_location
        #requests.post('http://localhost:5000/process_data', json=={"filename":file_location})
        #r = requests.post('http://localhost:5000/ingest_data', data=datas)
        #print(datas)
        saved = '<p style="color:green;">json data uploaded</p>'

        #return redirect(url_for('process_data.process', filename=file_location))
        render_template('index.html',saved=saved)
        return redirect(url_for("process_data.process",))