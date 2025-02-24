from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
from src.etl.email_utils import send_email  # Function to send email
from src.etl.insert_data_into_sql import insert_data_into_sql  # Function to insert data into SQL

# Function to execute the ETL script
def run_etl_script():
    send_email("ETL Process Started", "The ETL process has started.")  # Send start email
    subprocess.run(["python", "/app/src/etl/main.py"], check=True)

# Function to execute the upload to DataLake script
def run_upload_to_datalake():
    subprocess.run(["python", "/app/src/etl/upload_to_datalake.py"], check=True)
    send_email("ETL Process Finished", "The ETL process has completed and the data has been uploaded to DataLake.")  # Send finish email

# Function to insert data into SQL Server
def insert_data_to_sql():
    insert_data_into_sql()  # This function inserts the data into the SQL Server

# Define the DAG
with DAG('etl_dag',
         default_args={'owner': 'airflow', 'retries': 1},
         description='ETL process with Airflow',
         schedule_interval=None,  # Can be scheduled to run according to your needs
         start_date=datetime(2025, 2, 23),
         catchup=False) as dag:

    # Define the task that runs the ETL script
    run_etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_etl_script,
    )
    
    # Task to run the upload to DataLake
    run_upload_task = PythonOperator(
        task_id='run_upload_task',
        python_callable=run_upload_to_datalake,
    )

    # Task to insert data into SQL after DataLake upload
    insert_sql_task = PythonOperator(
        task_id='insert_sql_task',
        python_callable=insert_data_to_sql,
    )

    # Define task sequence
    run_etl_task >> run_upload_task >> insert_sql_task  # Ensures the SQL insert runs after both ETL and DataLake upload
