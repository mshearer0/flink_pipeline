FROM python:3.10-slim-bullseye

# Install Java & wget
RUN apt-get update && apt-get install -y default-jdk wget netcat && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create the lib directory inside the container's app folder
RUN mkdir -p /app/lib

# Download the 1.18 version (recommended for stability)
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar -P /app/lib/

# Set permissions
RUN chmod 755 /app/lib/flink-sql-connector-kafka-3.0.1-1.18.jar

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy everything else (respecting .dockerignore)
COPY . .

COPY wait-for-kafka.sh /app/wait-for-kafka.sh
RUN chmod +x /app/wait-for-kafka.sh

# Critical: allow Python to find the streaming_platform module
ENV PYTHONPATH=/app
