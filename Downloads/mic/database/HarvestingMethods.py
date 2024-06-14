import pandas as pd
import mysql.connector

def populate_harvesting_methods(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO HarvestingMethods (HarvestingMethodID, Method, StorageID)
                VALUES (%s, %s, %s)
            """, (row['HarvestingMethodID'], row['Method'], row['StorageID']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in HarvestingMethods table.")
                continue
            else:
                print(f"Error inserting into HarvestingMethods table: {e}")
                continue

def main():
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='', 
        database='coffee_sector'
    )
    cursor = conn.cursor()

    try:
        harvesting_methods_data = pd.read_csv('HarvestingMethods.csv')
        populate_harvesting_methods(cursor, harvesting_methods_data)
        conn.commit()
        print("HarvestingMethods table populated successfully.")
    except FileNotFoundError:
        print("HarvestingMethods.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
