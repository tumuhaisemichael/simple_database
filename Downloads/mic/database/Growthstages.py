import pandas as pd
import mysql.connector

def populate_growth_stages(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO GrowthStages (GrowthStageID, Stage, Time)
                VALUES (%s, %s, %s)
            """, (row['GrowthStageID'], row['Stage'], row['Time']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in GrowthStages table.")
                continue
            else:
                print(f"Error inserting into GrowthStages table: {e}")
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
        growth_stages_data = pd.read_csv('GrowthStages.csv')
        populate_growth_stages(cursor, growth_stages_data)
        conn.commit()
        print("GrowthStages table populated successfully.")
    except FileNotFoundError:
        print("GrowthStages.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
