import pandas as pd
import mysql.connector

def populate_diseases(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO Diseases (DiseaseID, Description, PartAffected)
                VALUES (%s, %s, %s)
            """, (row['DiseaseID'], row['Description'], row['PartAffected']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in Diseases table.")
                continue
            else:
                print(f"Error inserting into Diseases table: {e}")
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
        diseases_data = pd.read_csv('Diseases.csv')
        populate_diseases(cursor, diseases_data)
        conn.commit()
        print("Diseases table populated successfully.")
    except FileNotFoundError:
        print("Diseases.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
