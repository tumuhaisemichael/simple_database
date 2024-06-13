import pandas as pd
import mysql.connector

def populate_soil_requirements(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO SoilRequirements (SoilRequirementID, PH, SoilType, NutrientRequirements, SuitableVariety)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['SoilRequirementID'], row['PH'], row['SoilType'], row['NutrientRequirements'], row['SuitableVariety']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in SoilRequirements table.")
                continue
            else:
                print(f"Error inserting into SoilRequirements table: {e}")
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
        soil_requirements_data = pd.read_csv('SoilRequirements.csv')
        populate_soil_requirements(cursor, soil_requirements_data)
        conn.commit()
        print("SoilRequirements table populated successfully.")
    except FileNotFoundError:
        print("SoilRequirements.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
