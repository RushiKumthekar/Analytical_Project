from dagster import asset
import requests
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, inspect
import json

# **1. Web Scraping Asset**
@asset
def web_scraped_data():
    api_url = "https://api.openchargemap.io/v3/poi"
    api_key = "c608d5ad-5dea-47ae-9349-ea70c07f3c22"  # Replace with your actual API key
    params = {
        "key": api_key,
        "countrycode": "US",
        "maxresults": 50000,
        "compact": False,
        "verbose": True,
    }

    def get_unique_data(api_url, params, chunk_size=500):
        all_data = []
        unique_ids = set()
        total_records = params["maxresults"]
        pages = total_records // chunk_size
        if total_records % chunk_size > 0:
            pages += 1

        for page in range(1, pages + 1):
            params["page"] = page
            params["pagesize"] = chunk_size
            retries = 3
            while retries > 0:
                try:
                    response = requests.get(api_url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        new_entries = [entry for entry in data if entry['ID'] not in unique_ids]
                        all_data.extend(new_entries)
                        unique_ids.update(entry['ID'] for entry in new_entries)
                        if len(all_data) >= 50000:
                            return all_data[:50000]
                        break
                except requests.exceptions.RequestException as e:
                    retries -= 1
                    if retries == 0:
                        print(f"Skipping page {page} due to errors.")
        return all_data

    data = get_unique_data(api_url, params)
    return data

# **2. MongoDB Asset**
@asset
def mongodb_data(web_scraped_data):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB URI if different
    db = client['Analytics_Data']
    collection = db['web_scraped_data']

    # Create a unique index on 'UUID' to avoid duplicates
    collection.create_index("UUID", unique=True)

    # Use upsert to avoid duplicates (insert if not exists, update if exists)
    for doc in web_scraped_data:
        collection.update_one(
            {"UUID": doc["UUID"]},  # Match on UUID field
            {"$set": doc},  # Update the document if exists
            upsert=True  # Insert if the document doesn't exist
        )

    # Retrieve and return the data
    retrieved_data = list(collection.find())
    return retrieved_data
    
    
# **3. Clean and Process All Data**
from dagster import asset
import os
import pandas as pd

@asset
def cleaned_data(mongodb_data):
    # Define the `clean_dataframe` function
    def clean_dataframe(df, drop_all_missing=True, missing_threshold=90):
        if drop_all_missing:
            # Drop columns with all missing values
            df = df.dropna(axis=1, how='all')

        # Calculate the percentage of missing values for each column
        missing_percentage = df.isna().sum() / len(df) * 100

        # Drop columns exceeding the missing value threshold
        df = df.loc[:, missing_percentage <= missing_threshold]
        
        return df

    # Convert mongodb_data (which is a list of dicts) into a Pandas DataFrame
    mongodb_df = pd.DataFrame(mongodb_data)

    # Clean all datasets, including MongoDB data
    cleaned_mongodb_data = clean_dataframe(mongodb_df)
    
    # Clean other CSV data as well
    base_dir = r"C:\Users\hrush\newProject\newProject"
    washington_vehicle_population_path = os.path.join(base_dir, "Electric_Vehicle_Population_Data.csv")
    fuel_station_ev_path = os.path.join(base_dir, "alt_fuel_stations_ev_charging_units (Dec 4 2024).csv")
    fuel_station_all_path = os.path.join(base_dir, "alt_fuel_stations (Dec 4 2024).csv")

    # Check if files exist before reading
    if not os.path.exists(washington_vehicle_population_path):
        raise FileNotFoundError(f"File not found: {washington_vehicle_population_path}")
    if not os.path.exists(fuel_station_ev_path):
        raise FileNotFoundError(f"File not found: {fuel_station_ev_path}")
    if not os.path.exists(fuel_station_all_path):
        raise FileNotFoundError(f"File not found: {fuel_station_all_path}")

    # Read and clean the CSV files
    washington_vehicle_population = pd.read_csv(washington_vehicle_population_path)
    fuel_station_ev = pd.read_csv(fuel_station_ev_path)
    fuel_station_all = pd.read_csv(fuel_station_all_path)

    # Clean all datasets, including MongoDB data
    cleaned_washington_vehicle_population = clean_dataframe(washington_vehicle_population)
    cleaned_fuel_station_ev = clean_dataframe(fuel_station_ev)
    cleaned_fuel_station_all = clean_dataframe(fuel_station_all)

    return {
        "cleaned_mongodb_data": cleaned_mongodb_data,
        "cleaned_washington_vehicle_population": cleaned_washington_vehicle_population,
        "cleaned_fuel_station_ev": cleaned_fuel_station_ev,
        "cleaned_fuel_station_all": cleaned_fuel_station_all,
    }


# **4. Store All Data into PostgreSQL**
from sqlalchemy import create_engine, inspect
import pandas as pd
import json

@asset
def store_to_postgres(cleaned_data):
    engine = create_engine('postgresql+psycopg://postgres:admin@localhost:5432/Analytics', pool_size=10, max_overflow=20)

    def write_to_postgres(df, table_name):
        try:
            inspector = inspect(engine)
            if table_name in inspector.get_table_names():
                # Check for existing data and avoid duplicates
                with engine.connect() as connection:
                    existing_data = pd.read_sql(f"SELECT * FROM {table_name};", connection)
                    if not existing_data.empty:
                        merged = pd.concat([existing_data, df], ignore_index=True).drop_duplicates(keep=False)
                        df = merged.loc[~merged.index.isin(existing_data.index)]
                        if df.empty:
                            print(f"No new data to insert into {table_name}.")
                            return
            
            # Convert complex columns (dicts/lists) to JSON strings before insertion
            for column in df.columns:
                if df[column].apply(lambda x: isinstance(x, (dict, list))).any():
                    df[column] = df[column].apply(json.dumps)

            # Write data to PostgreSQL table
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"New data written to {table_name} successfully.")
        except Exception as e:
            print(f"Error writing to {table_name}: {e}")

    # Write cleaned data to PostgreSQL
    write_to_postgres(cleaned_data["cleaned_mongodb_data"], 'web_scraped_data')
    write_to_postgres(cleaned_data["cleaned_washington_vehicle_population"], 'washington_vehicle_population')
    write_to_postgres(cleaned_data["cleaned_fuel_station_ev"], 'fuel_station_ev')
    write_to_postgres(cleaned_data["cleaned_fuel_station_all"], 'fuel_station_all')

    return "Data successfully stored in PostgreSQL"
