FROM python:3.10-slim-bookworm

# Install Java JRE (required for Spark) and procps (required for Spark monitoring)
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark, kafka-python-ng (compatible with newer python/kafka interfaces), and psycopg2-binary
RUN pip install --no-cache-dir \
    pyspark==3.3.0 \
    kafka-python-ng \
    psycopg2-binary

WORKDIR /app

# The default command will be overridden or run manually
CMD ["python", "src/scripts/project_code.py"]
