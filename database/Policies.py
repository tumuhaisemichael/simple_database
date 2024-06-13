import pandas as pd
import mysql.connector

def populate_policies(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO Policies (PolicyID, Type, Details)
                VALUES (%s, %s, %s)
            """, (row['PolicyID'], row['Type'], row['Details']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in Policies table.")
                continue
            else:
                print(f"Error inserting into Policies table: {e}")
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
        policies_data = pd.read_csv('Policies.csv')
        populate_policies(cursor, policies_data)
        conn.commit()
        print("Policies table populated successfully.")
    except FileNotFoundError:
        print("Policies.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
