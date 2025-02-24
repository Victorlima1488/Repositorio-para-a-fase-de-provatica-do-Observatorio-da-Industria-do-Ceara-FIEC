# Choose a Python base image
FROM python:3.9-slim

# Add Debian Bullseye repository to ensure OpenJDK 11 is available
RUN echo "deb http://deb.debian.org/debian/ bullseye main" > /etc/apt/sources.list.d/bullseye.list && \
    apt-get update && apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set the working directory inside the container
WORKDIR /app

# Copy files from the src/etl directory to /app/src/etl inside the container
COPY src/etl /app/src/etl

# Copy the requirements.txt file
COPY requirements.txt /app/

# Install project dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set the command to run the main script of the project
CMD ["python", "/app/src/etl/main.py"]