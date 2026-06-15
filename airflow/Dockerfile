ARG AIRFLOW_IMAGE_NAME

# Base image sesuai versi Airflow di docker-compose.yml
FROM ${AIRFLOW_IMAGE_NAME}
# FROM apache/airflow:3.2.2

# Gunakan root untuk install package
# USER root
# RUN apt update && apt install tesseract-ocr tesseract-ocr-ind poppler-utils -y

# Balik ke user airflow (wajib untuk Airflow)
# USER airflow

# Copy file requirements.txt ke container
COPY requirements.txt /requirements.txt

# Install semua library dari requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# RUN pip install -r /requirements.txt

