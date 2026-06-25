FROM apache/airflow:2.7.2-python3.11

USER root
# Install Java JRE
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Copy local PySpark archive
COPY --chown=airflow:root pyspark-3.4.1.tar.gz /tmp/pyspark-3.4.1.tar.gz

# Install PySpark, FindSpark and Spark Provider
RUN pip install --no-cache-dir \
    /tmp/pyspark-3.4.1.tar.gz \
    findspark \
    apache-airflow-providers-apache-spark==4.1.3 && \
    rm /tmp/pyspark-3.4.1.tar.gz
