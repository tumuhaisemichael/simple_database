import pandas as pd
import mysql.connector

def populate_planting_methods(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO PlantingMethods (PlantingMethodID, Method, HowItsDone, Advantages, Disadvantages, SoilRequirementID)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['PlantingMethodID'], row['Method'], row['HowItsDone'], row['Advantages'], row['Disadvantages'], row['SoilRequirementID']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in PlantingMethods table.")
                continue
            else:
                print(f"Error inserting into PlantingMethods table: {e}")
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
        planting_methods_data = pd.read_csv('PlantingMethods.csv')
        populate_planting_methods(cursor, planting_methods_data)
        conn.commit()
        print("PlantingMethods table populated successfully.")
    except FileNotFoundError:
        print("PlantingMethods.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
