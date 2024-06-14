import pandas as pd
import mysql.connector

def populate_fertilization(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO Fertilization (FertilizationID, Types, ApplicationMethods, Frequency, SoilRequirementID, Advantages, Disadvantages, Contents)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['FertilizationID'], row['Types'], row['ApplicationMethods'], row['Frequency'], row['SoilRequirementID'], row['Advantages'], row['Disadvantages'], row['Contents']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in Fertilization table.")
                continue
            else:
                print(f"Error inserting into Fertilization table: {e}")
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
        fertilization_data = pd.read_csv('Fertilization.csv')
        populate_fertilization(cursor, fertilization_data)
        conn.commit()
        print("Fertilization table populated successfully.")
    except FileNotFoundError:
        print("Fertilization.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
