import os
from os.path import join, dirname, realpath
from flask import Flask, request, render_template, session
from flask import Blueprint
module_path = os.path.dirname(os.path.abspath(__file__))

print (os.getcwd())

app = Flask(__name__)


os.chdir(os.path.dirname(__file__))

#*** Flask configuration
 
# Define folder to save uploaded files to process further
UPLOAD_FOLDER = join(dirname(realpath(__file__)), 'uploads/')
 
# Define allowed files for uploading (for this example I want only csv file)
ALLOWED_EXTENSIONS = {'json'}
 
app = Flask(__name__, template_folder='templates', static_folder='static')
app.config["DEBUG"] = True

# Configure upload file path flask
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
 
# Define secret key to enable session
app.secret_key = 'btc2022'

## Register blueprints
from src.upload_json import uploads
from src.process_data import process_data
app.register_blueprint(uploads)
app.register_blueprint(process_data)

@app.route("/")
def main():
    """Function for rendering the main page"""
    
    return render_template('index.html')

#if __name__ == "__main__":
app.run()