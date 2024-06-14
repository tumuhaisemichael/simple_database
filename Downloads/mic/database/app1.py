
from flask import Flask, jsonify
import json

app = Flask(__name__)

# Path to the JSON file
JSON_FILE_PATH = 'database_file.json'

@app.route('/', methods=['GET'])
def get_database():
    try:
        with open(JSON_FILE_PATH, 'r') as f:
            database_data = json.load(f)
    except FileNotFoundError:
        database_data = {'error': 'Database file not found'}
    
    return jsonify(database_data)

if __name__ == '__main__':
    app.run(host = "0.0.0.0", port=8080,debug=True)

