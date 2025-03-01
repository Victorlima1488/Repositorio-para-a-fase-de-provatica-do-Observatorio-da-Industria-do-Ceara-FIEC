import os
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from etl.email_utils import send_email  # Make sure to import the email sending function

# Function to connect to SQL Server using SQLAlchemy
def connect_to_sql_server():
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')

    # Create SQLAlchemy engine for SQL Server
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')
    return engine

# Function to download data from Azure Blob Storage
def download_blob_data(container_name, blob_name, local_file_path):
    try:
        # Get environment variables for Azure credentials
        connection_string = os.getenv('AZURE_CONNECTION_STRING')  # Example: 'your_connection_string'
        
        # Initialize the Azure Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Download blob to local file
        blob_client = container_client.get_blob_client(blob_name)
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        
        print(f"File {blob_name} downloaded to {local_file_path}")
    except Exception as e:
        print(f"Error downloading blob: {e}")
        send_email("ETL Process - Azure Data Download Failed", f"Failed to download data from Azure. Error: {e}")

# Function to insert data into atracacao_fato table
def insert_atracacao_data(df):
    try:
        # Connect to SQL Server using SQLAlchemy engine
        engine = connect_to_sql_server()

        # Insert the data into atracacao_fato table
        df.to_sql('atracacao_fato', con=engine, if_exists='append', index=False)  # 'append' will add data without overwriting

        # Send success email after data insertion
        send_email("ETL Process - Atracacao Data Inserted", "Atracacao data has been successfully inserted into the atracacao_fato table.")
        print("Atracacao data inserted successfully into SQL Server.")
    
    except Exception as e:
        print(f"Error inserting Atracacao data: {e}")
        send_email("ETL Process - Data Insertion Failed", f"Failed to insert Atracacao data. Error: {e}")

# Function to insert data into carga_fato table
def insert_carga_data(df):
    try:
        # Connect to SQL Server using SQLAlchemy engine
        engine = connect_to_sql_server()

        # Insert the data into carga_fato table
        df.to_sql('carga_fato', con=engine, if_exists='append', index=False)  # 'append' will add data without overwriting

        # Send success email after data insertion
        send_email("ETL Process - Carga Data Inserted", "Carga data has been successfully inserted into the carga_fato table.")
        print("Carga data inserted successfully into SQL Server.")
    
    except Exception as e:
        print(f"Error inserting Carga data: {e}")
        send_email("ETL Process - Data Insertion Failed", f"Failed to insert Carga data. Error: {e}")

# Function to process data and insert into SQL Server
def process_and_insert_data():
    # Download the Azure data to local files (example, change blob names accordingly)
    download_blob_data("raw-data", "2021Atracacao.csv", "/tmp/2021Atracacao.csv")
    download_blob_data("raw-data", "2021Carga.csv", "/tmp/2021Carga.csv")
    
    # Read the data into DataFrames (change the file format if necessary, e.g., CSV or Parquet)
    atracacao_df = pd.read_csv("/tmp/2021Atracacao.csv")
    carga_df = pd.read_csv("/tmp/2021Carga.csv")

    # Insert data into atracacao_fato table
    insert_atracacao_data(atracacao_df)

    # Insert data into carga_fato table
    insert_carga_data(carga_df)

# Example to run the function
if __name__ == "__main__":
    process_and_insert_data()  # This will download data and insert into SQL Server