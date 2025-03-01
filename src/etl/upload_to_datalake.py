import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Fetch sensitive data from environment variables
connection_string = os.getenv('AZURE_CONNECTION_STRING')
container_name_processed = os.getenv('AZURE_CONTAINER_NAME_PROCESSED')  # Processed data container name

# Define the years for which the data will be uploaded
years = ["2021", "2022", "2023"]

# Connect to Blob service
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get the container client for the processed container
container_client_processed = blob_service_client.get_container_client(container_name_processed)

# Function to upload the file to the Data Lake
def upload_to_datalake(file_path, file_to_upload, container_client):
    blob_name = f"{file_to_upload}"  # Path inside the Data Lake

    # Upload the file to the Data Lake with overwrite set to True
    with open(file_path, "rb") as data:
        container_client.upload_blob(blob_name, data, overwrite=True)
        print(f"File {file_to_upload} uploaded to the Data Lake.")

# Iterate over the specified years for Atracacao data
for year in years:
    local_path_atracacao = os.path.join(r"/home/victor/repos/FIEC/data/processed_data/Atracao.parquet", f"{year}Atracacao")
    file_to_upload_atracacao = f"{year}Atracacao_data.parquet"  # File name based on the year
    file_path_atracacao = os.path.join(local_path_atracacao, file_to_upload_atracacao)

    if os.path.exists(file_path_atracacao):
        upload_to_datalake(file_path_atracacao, file_to_upload_atracacao, container_client_processed)
    else:
        print(f"The file {file_to_upload_atracacao} was not found in the directory {local_path_atracacao}.")

# Iterate over the specified years for Carga data
for year in years:
    local_path_raw = os.path.join(r"/home/victor/repos/FIEC/data/processed_data/Carga.parquet", f"{year}Carga")
    file_to_upload_raw = f"{year}Carga_data.parquet"  # File name based on the year
    file_path_raw = os.path.join(local_path_raw, file_to_upload_raw)

    if os.path.exists(file_path_raw):
        upload_to_datalake(file_path_raw, file_to_upload_raw, container_client_processed)
    else:
        print(f"The file {file_to_upload_raw} was not found in the directory {local_path_raw}.")

print("Upload process to processed-data container completed!")