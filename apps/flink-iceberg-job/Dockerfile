# Build the job jar first: ./gradlew build. Flink 2.x recommends Java 17 (21 is experimental).
FROM flink:2.1.3-java17

ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-2.1.3.jar
COPY build/libs/job.jar /opt/flink/usrlib/job.jar
