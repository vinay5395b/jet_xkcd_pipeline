FROM apache/airflow:2.8.1

# Install the postgres and requests libraries
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt