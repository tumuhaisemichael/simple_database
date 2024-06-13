import pandas as pd
import mysql.connector

def populate_government_policies(cursor, data):
    for index, row in data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO GovernmentPolicies (GovernmentPolicyID, PolicyID, Loans, MachineUse, Trainings, Taxes)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['GovernmentPolicyID'], row['PolicyID'], row['Loans'], row['MachineUse'], row['Trainings'], row['Taxes']))
        except mysql.connector.errors.IntegrityError as e:
            # Handle duplicate entry error (1062)
            if e.errno == 1062:
                print("Skipping duplicate entry in GovernmentPolicies table.")
                continue
            else:
                print(f"Error inserting into GovernmentPolicies table: {e}")
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
        government_policies_data = pd.read_csv('GovernmentPolicies.csv')
        populate_government_policies(cursor, government_policies_data)
        conn.commit()
        print("GovernmentPolicies table populated successfully.")
    except FileNotFoundError:
        print("GovernmentPolicies.csv not found.")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
