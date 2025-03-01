from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# Adiciona o diretório src ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from etl.email_utils import send_email  # Função para enviar email

# Função para executar o script ETL
def run_etl_script():
    subprocess.run(["python", "/app/src/etl/main.py"], check=True)

# Função para executar o upload para o DataLake
def run_upload_to_datalake():
    subprocess.run(["python", "/app/src/etl/upload_to_datalake.py"], check=True)

# Definição da DAG
with DAG('etl_dag',
         default_args={
             'owner': 'airflow',
             'retries': 1,
             'retry_delay': timedelta(minutes=5)
         },
         description='ETL process with Airflow',
         schedule_interval='* * * * *',  # Executa a cada minuto
         start_date=datetime(2025, 2, 27),
         catchup=False) as dag:

    # Tarefa que executa o script ETL
    run_etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_etl_script,
    )
    
    # Tarefa que executa o upload para o DataLake
    run_upload_task = PythonOperator(
        task_id='run_upload_task',
        python_callable=run_upload_to_datalake,
    )

    # Definição da sequência de tarefas
    run_etl_task >> run_upload_task  # Garante que o upload ocorra após o ETL