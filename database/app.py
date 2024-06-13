
# from flask import Flask, jsonify
# import mysql.connector

# app = Flask(__name__)

# # Database configuration
# db_config = {
#     'user': 'root',        # Replace with your MySQL username
#     'password': '',        # Replace with your MySQL password
#     'host': 'localhost',
#     'database': 'coffee_sector'
# }

# def get_db_connection():
#     connection = mysql.connector.connect(**db_config)
#     return connection

# @app.route('/', methods=['GET'])
# def home():
#     return "Welcome to the Coffee API. Access /api/database to get all data."

# @app.route('/api/database', methods=['GET'])
# def get_database():
#     connection = get_db_connection()
#     cursor = connection.cursor()
    
#     # Get all table names
#     cursor.execute("SHOW TABLES")
#     tables = cursor.fetchall()

#     database_data = {}
    
#     # Query data from each table
#     for (table_name,) in tables:
#         cursor.execute(f"SELECT * FROM {table_name}")
#         table_data = cursor.fetchall()
        
#         # Get column names
#         cursor.execute(f"SHOW COLUMNS FROM {table_name}")
#         columns = [column[0] for column in cursor.fetchall()]
        
#         # Convert to list of dictionaries
#         table_data_dicts = [dict(zip(columns, row)) for row in table_data]
        
#         database_data[table_name] = table_data_dicts
    
#     cursor.close()
#     connection.close()
    
#     return jsonify(database_data)

# if __name__ == '__main__':
#     app.run(debug=True)

from flask import Flask, jsonify
import mysql.connector

app = Flask(__name__)

# Database configuration
db_config = {
    'user': 'root',        # Replace with your MySQL username
    'password': '',        # Replace with your MySQL password
    'host': 'localhost',
    'database': 'coffee_sector'
}

def get_db_connection():
    connection = mysql.connector.connect(**db_config)
    return connection

@app.route('/', methods=['GET'])
def get_database():
    connection = get_db_connection()
    cursor = connection.cursor()
    
    # Get all table names
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    database_data = {}
    
    # Query data from each table
    for (table_name,) in tables:
        cursor.execute(f"SELECT * FROM {table_name}")
        table_data = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [column[0] for column in cursor.fetchall()]
        
        # Convert to list of dictionaries
        table_data_dicts = [dict(zip(columns, row)) for row in table_data]
        
        database_data[table_name] = table_data_dicts
    
    cursor.close()
    connection.close()
    
    return jsonify(database_data)

if __name__ == '__main__':
    app.run(debug=True)
