# Dockerfile for CiteConnect - Simple Version
FROM apache/airflow:2.9.2

USER root

# Install git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow

# Configure git
RUN git config --global user.email "aditya811.abhinav@gmail.com" && \
    git config --global user.name "Abhinav Aditya"

# Copy and install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
RUN pip install --no-cache-dir \
    asyncpg>=0.29.0 \
    sentence-transformers>=2.5.1 \
    transformers>=4.37.0 \
    torch>=2.1.0

# Copy everything (except what's in .dockerignore)
COPY --chown=airflow:root . /opt/airflow/dags/project/

# Set Python path
ENV PYTHONPATH="/opt/airflow/dags/project:${PYTHONPATH}"