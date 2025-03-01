from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, unix_timestamp, floor, format_string, trim, abs, when, year as pyspark_year, month as pyspark_month
)
import requests
import zipfile
import os
import io
import shutil
import re

def init_spark_session(app_name):
    """
    Initializes a Spark session.
    """
    return SparkSession.builder.master('local[*]').appName(app_name).getOrCreate()

def download_and_extract_zip(zip_url, save_dir):
    """
    Downloads a ZIP file, extracts it, and saves the text file inside the specified directory.
    """
    try:
        with requests.Session() as session:
            response = session.get(zip_url)
            response.raise_for_status()

            zip_file = io.BytesIO(response.content)
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                extracted_files = zip_ref.namelist()
                print(f"Extracted files: {extracted_files}")

                txt_file = next((f for f in extracted_files if f.endswith('.txt')), None)

                if not txt_file:
                    print("No TXT file found.")
                    return None

                txt_file_path = os.path.join(save_dir, txt_file)
                with zip_ref.open(txt_file) as file:
                    with open(txt_file_path, 'wb') as out_file:
                        out_file.write(file.read())

            return txt_file_path
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

def process_date_columns(df, date_columns):
    """
    Converts string date columns to timestamp format.
    """
    for column in date_columns:
        if column in df.columns:
            df = df.withColumn(column, to_timestamp(trim(col(column)), "dd/MM/yyyy HH:mm:ss"))
    return df

def calculate_time_difference(df, col1, col2, new_col):
    """
    Calculates the time difference between two date columns and creates a new column with the result.
    """
    if col1 in df.columns and col2 in df.columns:
        seconds_diff = abs(unix_timestamp(col(col2)) - unix_timestamp(col(col1)))
        df = df.withColumn(
            new_col,
            when(
                col(col1).isNull() | col(col2).isNull(), None
            ).otherwise(
                format_string("%02d:%02d:%02d", floor(seconds_diff / 3600), floor((seconds_diff % 3600) / 60), floor(seconds_diff % 60))
            )
        )
        return df
    return df

def save_dfs_to_parquet(dfs, output_path):
    """
    Saves a dictionary of DataFrames to the specified Parquet path.
    Creates directories if they don't exist and cleans up old files.
    """
    for year, df in dfs.items():
        # Create the output path for each year if it doesn't exist
        year_output_path = os.path.join(output_path, str(year))

        # Ensure the directory exists (create if it doesn't exist)
        os.makedirs(year_output_path, exist_ok=True)

        # Coalesce the DataFrame to a single partition for output
        single_partition_df = df.coalesce(1)

        # Remove any existing files in the directory
        if os.path.exists(year_output_path):
            # Only remove files within the directory, avoid removing the directory itself
            for file_name in os.listdir(year_output_path):
                file_path = os.path.join(year_output_path, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)  # Remove old files
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Remove old directories

        # Write the DataFrame as Parquet
        single_partition_df.write.mode("overwrite").parquet(year_output_path)

        # Rename the output file to match the expected format
        for filename in os.listdir(year_output_path):
            if filename.startswith("part-") and filename.endswith(".parquet"):
                part_file_path = os.path.join(year_output_path, filename)
                new_file_name = f"{year}_data.parquet"
                new_file_path = os.path.join(year_output_path, new_file_name)
                os.rename(part_file_path, new_file_path)
                print(f"Arquivo renomeado para: {new_file_path}")
                break

def process_files(antaq_links_download, raw_data_path, spark):
    """
    Downloads and processes the files from the provided links.
    """
    dfs_by_file = {}
    data_columns = [
        "Data Atracação", "Data Chegada", "Data Desatracação",
        "Data Início Operação", "Data Término Operação"
    ]

    for link in antaq_links_download:
        txt_file_path = download_and_extract_zip(link, raw_data_path)
        if txt_file_path:
            print(f"TXT file extracted: {txt_file_path}")
        else:
            print(f"Failed to extract file: {link}")

    # List and filter files
    all_raw_data_files = os.listdir(raw_data_path)
    all_txt_raw_data_files = sorted([f for f in all_raw_data_files if re.match(r"202\dAtracacao\.txt", f)])

    # Process each file
    for file_name in all_txt_raw_data_files:
        file_full_path = os.path.join(raw_data_path, file_name)
        df_name = os.path.splitext(file_name)[0]
        df = spark.read.option("delimiter", ";").csv(file_full_path, header=True, inferSchema=True)

        df_transformed = process_date_columns(df, data_columns)

        df_transformed = calculate_time_difference(df_transformed, "Data Atracação", "Data Chegada", "TEsperaAtracacao")
        df_transformed = calculate_time_difference(df_transformed, "Data Chegada", "Data Início Operação", "TEsperaInicioOp")
        df_transformed = calculate_time_difference(df_transformed, "Data Início Operação", "Data Término Operação", "TOperacao")
        df_transformed = calculate_time_difference(df_transformed, "Data Chegada", "Data Desatracação", "TEsperaDesatracacao")
        df_transformed = calculate_time_difference(df_transformed, "Data Atracação", "Data Desatracação", "TAtracado")
        df_transformed = calculate_time_difference(df_transformed, "Data Atracação", "Data Término Operação", "TEstadia")

        dfs_by_file[df_name] = df_transformed

    return dfs_by_file

def process_carga_and_atracacao(dfs_by_file, raw_data_path, spark):
    """
    Joins Carga and Atracacao DataFrames and stores them in a dictionary.
    """
    dfs_by_year = {}
    atracacao_dfs = {k: v for k, v in dfs_by_file.items() if "Atracacao" in k}

    all_files = os.listdir(raw_data_path)
    load_files = sorted([f for f in all_files if re.match(r"202\dCarga\.txt", f)])

    for load_file in load_files:
        year_match = re.search(r"(202\d)", load_file)
        if not year_match:
            continue
        year = year_match.group(1)
        atracacao_file = f"{year}Atracacao"

        if atracacao_file not in atracacao_dfs:
            print(f"Berthing DataFrame for {load_file} not found. Skipping...")
            continue

        carga_df = spark.read.option("delimiter", ";").csv(os.path.join(raw_data_path, load_file), header=True, inferSchema=True)
        atracacao_df = atracacao_dfs[atracacao_file]

        if "Data Início Operação" in atracacao_df.columns:
            atracacao_df = atracacao_df.withColumn("Ano_Inicio_Operacao", pyspark_year(col("Data Início Operação")))
            atracacao_df = atracacao_df.withColumn("Mes_Inicio_Operacao", pyspark_month(col("Data Início Operação")))

        df_final = carga_df.join(
            atracacao_df.select("IDAtracacao", "Ano_Inicio_Operacao", "Mes_Inicio_Operacao"),
            on="IDAtracacao",
            how="left"
        )

        dfs_by_year[year + "Carga"] = df_final

    return dfs_by_year

def main():
    # Setup Spark session
    spark = init_spark_session("ETL_ANTAQ")

    # Directory paths
    raw_data_path = os.path.join(os.getcwd(), 'data', 'raw_data')
    os.makedirs(raw_data_path, exist_ok=True)
    
    output_path = os.path.join(os.getcwd(), 'data', 'processed_data')
    os.makedirs(output_path, exist_ok=True)

    # Download, extract, and process files
    antaq_links_download = [
        "https://web3.antaq.gov.br/ea/txt/2021Atracacao.zip",
        "https://web3.antaq.gov.br/ea/txt/2021Carga.zip",
        "https://web3.antaq.gov.br/ea/txt/2022Atracacao.zip",
        "https://web3.antaq.gov.br/ea/txt/2022Carga.zip",
        "https://web3.antaq.gov.br/ea/txt/2023Atracacao.zip",
        "https://web3.antaq.gov.br/ea/txt/2023Carga.zip"
    ]

    # Process files and return DataFrames
    dfs_by_file = process_files(antaq_links_download, raw_data_path, spark)

    # Process Carga and Atracacao data
    dfs_by_year = process_carga_and_atracacao(dfs_by_file, raw_data_path, spark)

    # Save results to Parquet
    save_dfs_to_parquet(dfs_by_file, os.path.join(output_path, "Atracao.parquet"))
    save_dfs_to_parquet(dfs_by_year, os.path.join(output_path, "Carga.parquet"))

if __name__ == '__main__':
    main()