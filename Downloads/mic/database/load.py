import pandas as pd
import mysql.connector

# Function to insert data into Policies table
def populate_policies(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO Policies (PolicyID, Type, Details)
            VALUES (%s, %s, %s)
        """, (row['PolicyID'], row['Type'], row['Details']))

# Function to insert data into GovernmentPolicies table
def populate_government_policies(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO GovernmentPolicies (GovernmentPolicyID, PolicyID, Loans, MachineUse, Trainings, Taxes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row['GovernmentPolicyID'], row['PolicyID'], row['Loans'], row['MachineUse'], row['Trainings'], row['Taxes']))

# Function to insert data into Varieties table
def populate_varieties(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO Varieties (VarietyID, Name, PeriodOfGrowth, Description)
            VALUES (%s, %s, %s, %s)
        """, (row['VarietyID'], row['Name'], row['PeriodOfGrowth'], row['Description']))

# Function to insert data into SoilRequirements table
def populate_soil_requirements(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO SoilRequirements (SoilRequirementID, PH, SoilType, NutrientRequirements, SuitableVariety)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['SoilRequirementID'], row['PH'], row['SoilType'], row['NutrientRequirements'], row['SuitableVariety']))

# Function to insert data into PlantingMethods table
def populate_planting_methods(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO PlantingMethods (PlantingMethodID, Method, HowItsDone, Advantages, Disadvantages, SoilRequirementID)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row['PlantingMethodID'], row['Method'], row['HowItsDone'], row['Advantages'], row['Disadvantages'], row['SoilRequirementID']))

# Function to insert data into Fertilization table
def populate_fertilization(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO Fertilization (FertilizationID, Types, ApplicationMethods, Frequency, SoilRequirementID, Advantages, Disadvantages, Contents)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['FertilizationID'], row['Types'], row['ApplicationMethods'], row['Frequency'], row['SoilRequirementID'], row['Advantages'], row['Disadvantages'], row['Contents']))

# Function to insert data into GrowthStages table
def populate_growth_stages(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO GrowthStages (GrowthStageID, Stage, Time)
            VALUES (%s, %s, %s)
        """, (row['GrowthStageID'], row['Stage'], row['Time']))

# Function to insert data into Pests table
def populate_pests(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO Pests (PestID, Description, DiseasesCaused, PartAffected)
            VALUES (%s, %s, %s, %s)
        """, (row['PestID'], row['Description'], row['DiseasesCaused'], row['PartAffected']))

# Function to insert data into PestControlMethods table
def populate_pest_control_methods(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO PestControlMethods (PestControlMethodID, Method, Effectiveness, BestSuitedPest)
            VALUES (%s, %s, %s, %s)
        """, (row['PestControlMethodID'], row['Method'], row['Effectiveness'], row['BestSuitedPest']))

# Function to insert data into Diseases table
def populate_diseases(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO Diseases (DiseaseID, Description, PartAffected)
            VALUES (%s, %s, %s)
        """, (row['DiseaseID'], row['Description'], row['PartAffected']))

# Function to insert data into HarvestingMethods table
def populate_harvesting_methods(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO HarvestingMethods (HarvestingMethodID, Method, StorageID)
            VALUES (%s, %s, %s)
        """, (row['HarvestingMethodID'], row['Method'], row['StorageID']))

# Function to insert data into Storage table
def populate_storage(cursor, data):
    for index, row in data.iterrows():
        cursor.execute("""
            INSERT INTO Storage (StorageID, Method, Duration)
            VALUES (%s, %s, %s)
        """, (row['StorageID'], row['Method'], row['Duration']))

# Main function to read CSV and populate tables
def main():
    # Connect to MySQL database
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='', 
        database='coffee_sector'
    )
    cursor = conn.cursor()

    # Read CSV files
    policies_data = pd.read_csv('Policies.csv')
    government_policies_data = pd.read_csv('GovernmentPolicies.csv')
    varieties_data = pd.read_csv('Varieties.csv')
    soil_requirements_data = pd.read_csv('SoilRequirements.csv')
    planting_methods_data = pd.read_csv('PlantingMethods.csv')
    fertilization_data = pd.read_csv('Fertilization.csv')
    growth_stages_data = pd.read_csv('GrowthStages.csv')
    pests_data = pd.read_csv('Pests.csv')
    pest_control_methods_data = pd.read_csv('PestControlMethods.csv')
    diseases_data = pd.read_csv('Diseases.csv')
    harvesting_methods_data = pd.read_csv('HarvestingMethods.csv')
    storage_data = pd.read_csv('Storage.csv')

    # Populate tables
    populate_policies(cursor, policies_data)
    populate_government_policies(cursor, government_policies_data)
    populate_varieties(cursor, varieties_data)
    populate_soil_requirements(cursor, soil_requirements_data)
    populate_planting_methods(cursor, planting_methods_data)
    populate_fertilization(cursor, fertilization_data)
    populate_growth_stages(cursor, growth_stages_data)
    populate_pests(cursor, pests_data)
    populate_pest_control_methods(cursor, pest_control_methods_data)
    populate_diseases(cursor, diseases_data)
    populate_harvesting_methods(cursor, harvesting_methods_data)
    populate_storage(cursor, storage_data)

    # Commit the transaction
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
