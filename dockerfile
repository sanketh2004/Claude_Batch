FROM apache/airflow:2.7.2

USER root

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create necessary directories
USER root
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed /opt/airflow/spark_jobs
RUN chown -R airflow: /opt/airflow/data /opt/airflow/spark_jobs

USER airflow