# Use an official Airflow image as the base
FROM apache/airflow:2.9.1-python3.8

# Copy the requirements.txt file into the Docker image
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt
