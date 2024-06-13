import pandas as pd
import mysql.connector

def populate_storage(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO Storage (StorageID, Method, Duration)
                VALUES (%s, %s, %s)
            """, (row['StorageID'], row['Method'], row['Duration']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in Storage table.")
                continue
            else:
                print(f"Error inserting into Storage table: {e}")
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
        storage_data = pd.read_csv('Storage.csv')
        populate_storage(cursor, storage_data)
        conn.commit()
        print("Storage table populated successfully.")
    except FileNotFoundError:
        print("Storage.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
