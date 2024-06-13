import pandas as pd
import mysql.connector

# Function to insert data into a table with error handling for duplicates
def populate_table(cursor, data, table_name):
    for index, row in data.iterrows():
        try:
            cursor.execute(f"INSERT INTO {table_name} ({', '.join(row.keys())}) VALUES ({', '.join(['%s'] * len(row))})", tuple(row))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print(f"Skipping duplicate entry for table '{table_name}': {row}")
                continue
            else:
                # If it's another IntegrityError, print the error and continue to the next row
                print(f"Error inserting into table '{table_name}': {e}")
                continue

# Main function to read CSV and populate tables
def main():
    # Connect to MySQL database
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='', 
        database='coffee_sector'
    )
    cursor = conn.cursor()

    # Define table names
    table_names = [
        'Policies', 'GovernmentPolicies', 'Varieties', 'SoilRequirements',
        'PlantingMethods', 'Fertilization', 'GrowthStages', 'Pests',
        'PestControlMethods', 'Diseases', 'HarvestingMethods', 'Storage'
    ]

    # Read CSV files and populate tables
    for table_name in table_names:
        try:
            table_data = pd.read_csv(f'{table_name}.csv')
            populate_table(cursor, table_data, table_name)
        except FileNotFoundError:
            print(f"CSV file '{table_name}.csv' not found.")
            continue

    # Commit the transaction
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
