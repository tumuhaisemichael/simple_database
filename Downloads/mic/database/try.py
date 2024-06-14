import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
import mysql.connector

# Database connection details
db_user = 'root'
db_password = ''
db_host = 'localhost'  # or your host
db_name = 'coffee_sector'

# Create a connection string
connection_string = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}'

# Create an engine
engine = create_engine(connection_string)

# Function to insert data into Policies table
def populate_policies(engine, data):
    data.to_sql('Policies', engine, if_exists='append', index=False)

# Function to insert data into GovernmentPolicies table
def populate_government_policies(engine, data):
    data.to_sql('GovernmentPolicies', engine, if_exists='append', index=False)

# Function to insert data into Varieties table
def populate_varieties(engine, data):
    data.to_sql('Varieties', engine, if_exists='append', index=False)

# Function to insert data into SoilRequirements table
def populate_soil_requirements(engine, data):
    data.to_sql('SoilRequirements', engine, if_exists='append', index=False)

# Function to insert data into PlantingMethods table
def populate_planting_methods(engine, data):
    data.to_sql('PlantingMethods', engine, if_exists='append', index=False)

# Function to insert data into Fertilization table
def populate_fertilization(engine, data):
    data.to_sql('Fertilization', engine, if_exists='append', index=False)

# Function to insert data into GrowthStages table
def populate_growth_stages(engine, data):
    data.to_sql('GrowthStages', engine, if_exists='append', index=False)

# Function to insert data into Pests table
def populate_pests(engine, data):
    data.to_sql('Pests', engine, if_exists='append', index=False)

# Function to insert data into PestControlMethods table
def populate_pest_control_methods(engine, data):
    data.to_sql('PestControlMethods', engine, if_exists='append', index=False)

# Function to insert data into Diseases table
def populate_diseases(engine, data):
    data.to_sql('Diseases', engine, if_exists='append', index=False)

# Function to insert data into HarvestingMethods table
def populate_harvesting_methods(engine, data):
    data.to_sql('HarvestingMethods', engine, if_exists='append', index=False)

# Function to insert data into Storage table
def populate_storage(engine, data):
    data.to_sql('Storage', engine, if_exists='append', index=False)

# Main function to read CSV and populate tables
def main():
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
    populate_policies(engine, policies_data)
    populate_government_policies(engine, government_policies_data)
    populate_varieties(engine, varieties_data)
    populate_soil_requirements(engine, soil_requirements_data)
    populate_planting_methods(engine, planting_methods_data)
    populate_fertilization(engine, fertilization_data)
    populate_growth_stages(engine, growth_stages_data)
    populate_pests(engine, pests_data)
    populate_pest_control_methods(engine, pest_control_methods_data)
    populate_diseases(engine, diseases_data)
    populate_harvesting_methods(engine, harvesting_methods_data)
    populate_storage(engine, storage_data)

if __name__ == '__main__':
    main()
