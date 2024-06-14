import pandas as pd
import mysql.connector

def populate_pest_control_methods(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO PestControlMethods (PestControlMethodID, Method, Effectiveness, BestSuitedPest)
                VALUES (%s, %s, %s, %s)
            """, (row['PestControlMethodID'], row['Method'], row['Effectiveness'], row['BestSuitedPest']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in PestControlMethods table.")
                continue
            else:
                print(f"Error inserting into PestControlMethods table: {e}")
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
        pest_control_methods_data = pd.read_csv('PestControlMethods.csv')
        populate_pest_control_methods(cursor, pest_control_methods_data)
        conn.commit()
        print("PestControlMethods table populated successfully.")
    except FileNotFoundError:
        print("PestControlMethods.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
