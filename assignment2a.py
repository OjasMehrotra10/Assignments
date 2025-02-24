import pandas as pd
import logging
import os
from dotenv import load_dotenv
from ucimlrepo import fetch_ucirepo
from sqlalchemy import create_engine, inspect, text

# * configuring the logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()

# * Function to Load the Data from the repository.
def load_data():
    try:
        logging.info("Fetching Auto MPG dataset from UCI repository...")
        auto_mpg = fetch_ucirepo(id=9)
        df = pd.concat([auto_mpg.data.features, auto_mpg.data.targets], axis=1)
        logging.info("Dataset successfully loaded.")
        return df
    except Exception as e:
        logging.error(f"Error loading dataset: {e}")
        return None

# * Function to Clean dataset
def clean_data(df):
    try:
        logging.info("Cleaning dataset...")
        
        df.replace("?", pd.NA, inplace=True)
        
        df.dropna(inplace=True)

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        categorical_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

        for col in categorical_cols:
            df[col] = df[col].astype("category")

        logging.info("Data cleaning completed.")
        return df
    except Exception as e:
        logging.error(f"Error cleaning dataset: {e}")
        return None

# * Function to Save dataset to CSV Format
def save_to_csv(df, filename="cleaned_auto_mpg.csv"):
    try:
        df.to_csv(filename, index=False)
        logging.info(f"Dataset saved as {filename}")
    except Exception as e:
        logging.error(f"Error saving dataset to CSV: {e}")

# * Function to Save dataset to JSON Format
def save_to_json(df, filename="cleaned_auto_mpg.json"):
    try:
        df.to_json(filename, orient="records")
        logging.info(f"Dataset saved as {filename}")
    except Exception as e:
        logging.error(f"Error saving dataset to JSON: {e}")

# * Function to Connect to PostgreSQL by using env config variables
def get_db_connection():
    try:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_host, db_port, db_name]):
            raise ValueError("Database credentials are missing in the .env file.")

        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        logging.info("Connected to PostgreSQL database.")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None

# * Function to upload the loaded , cleaned data into the PostgreSQL table
def upload_to_postgres(df, table_name="auto_mpg", schema_name="customer_data"):
    try:
        engine = get_db_connection()
        if engine is None:
            return

        with engine.connect():
            logging.info("Creating table / inserting data...")
            df.to_sql(table_name, engine, schema=schema_name,  if_exists="replace", index=False)

        logging.info(f"Data successfully uploaded to PostgreSQL table '{table_name}' inside the schema '{schema_name}'.")
    except Exception as e:
        logging.error(f"Error uploading data to PostgreSQL: {e}")

# * Main function to make a systematic approach to the functions
def main():
    df = load_data()
    if df is not None:
        df = clean_data(df)
        save_to_csv(df)
        save_to_json(df)
        upload_to_postgres(df)

if __name__ == "__main__":
    main()
