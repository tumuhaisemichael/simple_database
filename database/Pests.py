# import pandas as pd
# import mysql.connector
# def populate_pests(cursor, data):
#     for index, row in data.iterrows():
#         if pd.isna(row).any():
#             print(f"Skipping row with missing data: {row}")
#             continue
#         try:
#             cursor.execute("""
#                 INSERT INTO Pests (PestID, Description, DiseasesCaused, PartAffected)
#                 VALUES (%s, %s, %s, %s)
#             """, (row['PestID'], row['Description'], row['DiseasesCaused'], row['PartAffected']))
#         except mysql.connector.errors.IntegrityError as e:
#             if e.errno == 1062:
#                 print("Skipping duplicate entry in Pests table.")
#                 continue
#             else:
#                 print(f"Error inserting into Pests table: {e}")
#                 continue
# def main():
#     conn = mysql.connector.connect(
#         host='localhost',
#         user='root',
#         password='', 
#         database='coffee_sector'
#     )
#     cursor = conn.cursor()

#     try:
#         pests_data = pd.read_csv('Pests.csv')
#         populate_pests(cursor, pests_data)
#         conn.commit()
#         print("Pests table populated successfully.")
#     except FileNotFoundError:
#         print("Pests.csv not found.")
#     finally:
#         cursor.close()
#         conn.close()

# if __name__ == '__main__':
#     main()

import pandas as pd
import mysql.connector

def populate_pests(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO Pests (PestID, Description, DiseasesCaused, PartAffected)
                VALUES (%s, %s, %s, %s)
            """, (row['PestID'], row['Description'], row['DiseasesCaused'], row['PartAffected']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in Pests table.")
                continue
            else:
                print(f"Error inserting into Pests table: {e}")
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
        pests_data = pd.read_csv('Pests.csv')
        populate_pests(cursor, pests_data)
        conn.commit()
        print("Pests table populated successfully.")
    except FileNotFoundError:
        print("Pests.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
