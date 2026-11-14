# Build the distribution first:  ./gradlew installDist
FROM eclipse-temurin:21-jre

RUN useradd --system --uid 10001 --no-create-home app
COPY build/install/iceberg-exporter /opt/iceberg-exporter

# Iceberg logs a scan report on every inspection; keep its logging at WARN.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError -Dorg.slf4j.simpleLogger.log.org.apache.iceberg=warn"
USER 10001
EXPOSE 9400
ENTRYPOINT ["/opt/iceberg-exporter/bin/iceberg-exporter"]
