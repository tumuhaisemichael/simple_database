import pandas as pd
import mysql.connector

def populate_varieties(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO Varieties (VarietyID, Name, PeriodOfGrowth, Description)
                VALUES (%s, %s, %s, %s)
            """, (row['VarietyID'], row['Name'], row['PeriodOfGrowth'], row['Description']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in Varieties table.")
                continue
            else:
                print(f"Error inserting into Varieties table: {e}")
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
        varieties_data = pd.read_csv('Varieties.csv')
        populate_varieties(cursor, varieties_data)
        conn.commit()
        print("Varieties table populated successfully.")
    except FileNotFoundError:
        print("Varieties.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
