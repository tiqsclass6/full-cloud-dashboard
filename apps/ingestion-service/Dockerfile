# Build first: ./gradlew :service:installDist :load-generator:installDist. The JDK base keeps jcmd/jfr for profiling.
FROM eclipse-temurin:21-jdk

# Ubuntu security updates on top of the base image (apt, ... between base rebuilds).
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --no-create-home app

# OpenTelemetry Java agent, pinned by checksum. mkdir first: ADD --chmod would give the new directory 0644.
ARG OTEL_AGENT_VERSION=2.32.0
RUN mkdir -p /opt/otel
ADD --chmod=0644 --checksum=sha256:f787eb6c7f3d18e69a431e108a15278d25ee37f83d68b678f621e063f3988f82 \
    https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/${OTEL_AGENT_VERSION}/opentelemetry-javaagent-${OTEL_AGENT_VERSION}.jar \
    /opt/otel/opentelemetry-javaagent.jar
COPY service/build/install/ingestion-service /opt/ingestion-service
COPY load-generator/build/install/loadgen /opt/loadgen

# Container-aware heap sizing; extra flags (GC choice, JFR) are supplied through JAVA_OPTS.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
USER 10001
EXPOSE 8080 8081
ENTRYPOINT ["/opt/ingestion-service/bin/ingestion-service"]
