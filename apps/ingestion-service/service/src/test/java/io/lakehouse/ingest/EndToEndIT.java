package io.lakehouse.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Full path against a real Kafka broker. Run with {@code ./gradlew integrationTest} (needs Docker). */
@Tag("integration")
@Testcontainers
class EndToEndIT {

    private static final String TOPIC = "events.raw";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    private static HttpServer registry;
    private static IngestService service;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    @SuppressWarnings("null")
    static void start() throws Exception {
        Properties admin = new Properties();
        admin.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient client = AdminClient.create(admin)) {
            client.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }

        // Stand-in for Apicurio: accepts any registration and returns schema id 11.
        registry = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        registry.createContext("/subjects", exchange -> {
            byte[] body = "{\"id\":11}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        registry.start();

        Config config = new Config(KAFKA.getBootstrapServers(),
                "http://127.0.0.1:" + registry.getAddress().getPort(), TOPIC, 0, 0, 1000, 500, 1 << 20,
                Duration.ofSeconds(15), Map.of());
        service = new IngestService(config);
        service.start();
    }

    @AfterAll
    static void stop() {
        if (service != null) {
            service.close();
        }
        if (registry != null) {
            registry.stop(0);
        }
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void publishesAcknowledgedAvroEventsToKafkaAndExposesMetrics() throws Exception {
        assertThat(get(service.managementPort(), "/health/ready").statusCode()).isEqualTo(200);

        StringBuilder body = new StringBuilder("[");
        for (int i = 0; i < 50; i++) {
            body.append(i == 0 ? "" : ",")
                    .append("{\"source\":\"src-").append(i % 5).append("\",\"type\":\"cpu.usage\",\"value\":")
                    .append(i).append(".5}");
        }
        body.append("]");
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + service.ingestPort() + "/v1/events"))
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
        String responseBody = response.body();
        // JUnit's assertEquals for messages: AssertJ's chained .as(...) trips the IDE's null analysis.
        assertEquals(200, response.statusCode(), String.valueOf(responseBody));

        EventSchema schema = new EventSchema(java.time.Clock.systemUTC());
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + System.nanoTime());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        List<ConsumerRecord<String, byte[]>> received = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (received.size() < 50 && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
            }
        }

        assertThat(received).hasSize(50);
        Set<String> ids = new HashSet<>();
        Map<String, Integer> partitionBySource = new HashMap<>();
        for (ConsumerRecord<String, byte[]> record : received) {
            GenericRecord decoded = ConfluentAvroEncoderTest.decode(schema, record.value());
            assertThat(record.value()[0]).isEqualTo((byte) 0);
            Object eventId = decoded.get("event_id");
            String eventIdString = java.util.Objects.requireNonNull(eventId, "event_id").toString();
            ids.add(eventIdString);
            Object source = decoded.get("source");
            String key = java.util.Objects.requireNonNull(record.key(), "record key");
            assertThat(java.util.Objects.requireNonNull(source, "source").toString())
                    .isEqualTo(key);
            int partition = record.partition();
            Integer previous = partitionBySource.put(key, partition);
            if (previous != null) {
                assertEquals(partition, previous.intValue(), "all events of one source stay on one partition");
            }
        }
        assertThat(ids).hasSize(50);

        String metrics = get(service.managementPort(), "/metrics").body();
        assertThat(metrics).contains(
                "ingest_events_total{application=\"ingestion-service\",outcome=\"published\"} 50.0",
                "jvm_memory_used_bytes",
                "kafka_producer_");
    }
}
