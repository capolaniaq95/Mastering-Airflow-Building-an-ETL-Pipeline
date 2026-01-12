FROM apache/airflow:3.1.5-python3.11

# Install additional system dependencies if needed
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow