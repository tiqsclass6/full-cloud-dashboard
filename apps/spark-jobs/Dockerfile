# PySpark jobs with the Iceberg runtime and AWS bundle baked in (no downloads at start). Context: apps/spark-jobs
FROM apache/spark:4.1.4-scala2.13-java21-python3-ubuntu

USER root
# Ubuntu security updates on top of the pinned base image (apt, linux-libc-dev, ... between base rebuilds).
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
ARG ICEBERG_VERSION=1.12.0
ADD --chmod=0644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=0644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
# The reconcile reads Kafka's offset index (Admin client only; the same version as the ingestion service).
ARG KAFKA_CLIENTS_VERSION=4.3.1
ADD --chmod=0644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/

COPY lakehouse_jobs /opt/jobs/lakehouse_jobs
COPY jobs/ /opt/jobs/
USER spark
