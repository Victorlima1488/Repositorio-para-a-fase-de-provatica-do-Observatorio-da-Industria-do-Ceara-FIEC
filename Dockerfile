# Use a versão slim do Python 3.9
FROM python:3.9-slim

# Instalar dependências básicas
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk ca-certificates-java \
    build-essential libpq-dev && \
    apt-get clean

# Configurar variável de ambiente para o Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Criar diretório de trabalho compatível com Airflow
WORKDIR /opt/airflow

# Copiar os arquivos do projeto para dentro do contêiner
COPY src /opt/airflow/src
COPY dags /opt/airflow/dags
COPY requirements.txt /opt/airflow/

# Adicionar o caminho do projeto ao PYTHONPATH para evitar erro de importação
ENV PYTHONPATH="/opt/airflow:/opt/airflow/src:/opt/airflow/dags"

# Instalar dependências do Python
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Ajustar permissões para evitar problemas de acesso
RUN chmod -R 777 /opt/airflow/dags /opt/airflow/src

# Definir o comando padrão do contêiner (pode ser alterado conforme necessidade)
CMD ["python", "/opt/airflow/src/etl/main.py"]