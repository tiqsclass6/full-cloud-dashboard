package io.lakehouse.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JobConfigTest {

    @Test
    void localDefaultsUseTheRestCatalogWithAnS3Endpoint() {
        JobConfig config = JobConfig.fromEnv(Map.of(
                "S3_ENDPOINT", "http://s3.data.svc:8333", "S3_ACCESS_KEY_ID", "id", "S3_SECRET_ACCESS_KEY", "secret"));

        assertThat(config.catalogImpl()).isEqualTo(JobConfig.REST_CATALOG);
        assertThat(config.catalogProperties())
                .containsEntry("uri", "http://iceberg-rest.data.svc:8181")
                .containsEntry("s3.endpoint", "http://s3.data.svc:8333")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.access-key-id", "id");
    }

    @Test
    void glueUsesTheGlueCatalogAndTheDefaultCredentialChain() {
        JobConfig config = JobConfig.fromEnv(Map.of(
                "ICEBERG_CATALOG_TYPE", "glue", "ICEBERG_WAREHOUSE", "s3://lakehouse-dev-warehouse/iceberg"));

        assertThat(config.catalogImpl()).isEqualTo(JobConfig.GLUE_CATALOG);
        assertThat(config.catalogProperties())
                .containsEntry("warehouse", "s3://lakehouse-dev-warehouse/iceberg")
                .containsEntry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .doesNotContainKeys("uri", "s3.endpoint", "s3.access-key-id", "s3.path-style-access");
    }

    @Test
    void kafkaClientVariablesBecomeConsumerProperties() {
        JobConfig config = JobConfig.fromEnv(Map.of(
                "KAFKA_CLIENT_SECURITY_PROTOCOL", "SSL",
                "KAFKA_CLIENT_SSL_KEYSTORE_LOCATION", "/etc/kafka-client/user/user.p12",
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9093"));

        assertThat(config.kafkaClientProperties())
                .containsEntry("security.protocol", "SSL")
                .containsEntry("ssl.keystore.location", "/etc/kafka-client/user/user.p12")
                .doesNotContainKey("bootstrap.servers")
                .hasSize(2);
    }

    @Test
    void noKafkaClientVariablesMeansNoExtraProperties() {
        assertThat(JobConfig.fromEnv(Map.of()).kafkaClientProperties()).isEmpty();
    }

    @Test
    void rejectsUnknownCatalogTypes() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> JobConfig.fromEnv(Map.of("ICEBERG_CATALOG_TYPE", "hive")))
                .withMessageContaining("rest or glue");
    }
}
