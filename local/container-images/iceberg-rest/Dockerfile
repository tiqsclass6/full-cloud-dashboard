# Adds the PostgreSQL JDBC driver so the REST catalog can keep its state in Postgres.
FROM apache/iceberg-rest-fixture:1.10.1

ARG PG_JDBC_VERSION=42.7.4
ADD --chmod=0644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar /usr/lib/iceberg-rest/postgresql.jar

# Put both jars on the classpath (java -jar would ignore extra jars).
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
