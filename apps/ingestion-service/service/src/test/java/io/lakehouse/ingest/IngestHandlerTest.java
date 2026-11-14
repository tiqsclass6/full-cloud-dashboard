package io.lakehouse.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Exercises the real HTTP server and ingest pipeline against a MockProducer (no Kafka needed). */
class IngestHandlerTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    private MockProducer<String, byte[]> producer;
    private IngestServer server;
    private Semaphore permits;
    private EventSchema schema;
    private int maxInFlight;

    private void start(boolean autoComplete, int maxInFlight, int maxBody, Duration publishTimeout) throws IOException {
        this.maxInFlight = maxInFlight;
        Clock clock = Clock.systemUTC();
        Config config = new Config("unused:9092", "http://unused", "events.raw", 0, 0, maxInFlight, Math.min(100, maxInFlight), maxBody,
                publishTimeout, Map.of());
        // A null partitioner makes MockProducer fall back to its default (key hash) partitioning.
        producer = new MockProducer<>(autoComplete, (Partitioner) null, new StringSerializer(),
                new ByteArraySerializer());
        schema = new EventSchema(clock);
        permits = new Semaphore(maxInFlight);
        IngestMetrics metrics = new IngestMetrics(new SimpleMeterRegistry(), permits, maxInFlight);
        EventIngestor ingestor = new EventIngestor(producer, new ConfluentAvroEncoder(schema.schema(), 7), schema,
                new EventValidator(clock), metrics, permits, config);
        server = new IngestServer(0, new IngestHandler(ingestor, metrics, maxBody));
        server.start();
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private HttpRequest.Builder request() {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + "/v1/events"))
                .header("Content-Type", "application/json");
    }

    private HttpResponse<String> post(String body) throws Exception {
        return http.send(request().POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private CompletableFuture<HttpResponse<String>> postAsync(String body) {
        return http.sendAsync(request().POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private void awaitSends(int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (producer.history().size() < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("producer did not receive " + count + " records in time");
            }
            Thread.sleep(10);
        }
    }

    @Test
    void acceptsSingleEventAndPublishesAvroToKafkaKeyedBySource() throws Exception {
        start(true, 100, 1 << 20, Duration.ofSeconds(5));

        HttpResponse<String> response = post("{\"source\":\"host-1\",\"type\":\"cpu.usage\",\"value\":0.9,"
                + "\"attributes\":{\"region\":\"us-east-1\"}}");

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode json = JSON.readTree(response.body());
        assertThat(json.get("accepted").asInt()).isEqualTo(1);
        assertThat(producer.history()).hasSize(1);
        var sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo("events.raw");
        assertThat(sent.key()).isEqualTo("host-1");
        GenericRecord decoded = ConfluentAvroEncoderTest.decode(schema, sent.value());
        assertThat(decoded.get("event_id").toString()).isEqualTo(json.get("event_ids").get(0).asText());
        assertThat(decoded.get("value")).isEqualTo(0.9);
        assertThat(permits.availablePermits()).isEqualTo(maxInFlight);
    }

    @Test
    void acceptsBatchAndKeepsClientSuppliedEventIds() throws Exception {
        start(true, 100, 1 << 20, Duration.ofSeconds(5));
        String id = "3f2b8c1e-5c1a-4c55-9a53-1b2f6b1f0c11";

        HttpResponse<String> response = post("[{\"event_id\":\"" + id + "\",\"source\":\"a\",\"type\":\"t\"},"
                + "{\"source\":\"b\",\"type\":\"t\"},{\"source\":\"c\",\"type\":\"t\"}]");

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode ids = JSON.readTree(response.body()).get("event_ids");
        assertThat(ids).hasSize(3);
        assertThat(ids.get(0).asText()).isEqualTo(id);
        assertThat(producer.history()).hasSize(3);
    }

    @Test
    void rejectsInvalidEventsWithoutPublishingAnything() throws Exception {
        start(true, 100, 1 << 20, Duration.ofSeconds(5));

        HttpResponse<String> response = post("[{\"source\":\"ok\",\"type\":\"t\"},{\"type\":\"t\"}]");

        assertThat(response.statusCode()).isEqualTo(400);
        String responseBody = java.util.Objects.requireNonNull(response.body());
        assertThat(responseBody).contains("validation_failed");
        assertThat(responseBody).contains("events[1]: source is required");
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void rejectsMalformedJsonAndUnknownFields() throws Exception {
        start(true, 100, 1 << 20, Duration.ofSeconds(5));

        assertThat(post("{not json").statusCode()).isEqualTo(400);
        HttpResponse<String> unknown = post("{\"source\":\"a\",\"type\":\"t\",\"bogus\":1}");
        assertThat(unknown.statusCode()).isEqualTo(400);
        assertThat(unknown.body()).contains("malformed_json");
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void rejectsOtherMethodsAndOversizedBodies() throws Exception {
        start(true, 100, 100, Duration.ofSeconds(5));

        HttpResponse<String> get = http.send(request().GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(get.statusCode()).isEqualTo(405);
        assertThat(get.headers().firstValue("Allow")).contains("POST");

        HttpResponse<String> big = post("{\"source\":\"a\",\"type\":\"t\",\"attributes\":{\"k\":\"" + "x".repeat(200) + "\"}}");
        assertThat(big.statusCode()).isEqualTo(413);
    }

    @Test
    void appliesBackPressureWhenInFlightLimitIsReached() throws Exception {
        start(false, 1, 1 << 20, Duration.ofSeconds(10));

        CompletableFuture<HttpResponse<String>> first = postAsync("{\"source\":\"a\",\"type\":\"t\"}");
        awaitSends(1);

        HttpResponse<String> second = post("{\"source\":\"b\",\"type\":\"t\"}");
        assertThat(second.statusCode()).isEqualTo(429);
        assertThat(second.headers().firstValue("Retry-After")).contains("1");
        assertThat(producer.history()).hasSize(1);

        producer.completeNext();
        assertThat(first.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
        assertThat(permits.availablePermits()).isEqualTo(1);

        CompletableFuture<HttpResponse<String>> third = postAsync("{\"source\":\"c\",\"type\":\"t\"}");
        awaitSends(2);
        producer.completeNext();
        assertThat(third.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
    }

    @Test
    void returns503AndReleasesPermitsWhenKafkaFails() throws Exception {
        start(false, 10, 1 << 20, Duration.ofSeconds(10));

        CompletableFuture<HttpResponse<String>> pending = postAsync("{\"source\":\"a\",\"type\":\"t\"}");
        awaitSends(1);
        producer.errorNext(new RuntimeException("broker unavailable"));

        HttpResponse<String> response = pending.get(5, TimeUnit.SECONDS);
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.headers().firstValue("Retry-After")).contains("2");
        assertThat(response.body()).contains("publish_failed");
        assertThat(permits.availablePermits()).isEqualTo(10);
    }

    @Test
    void partialFailureReturnsEveryIdSoARetryReusesThem() throws Exception {
        start(false, 10, 1 << 20, Duration.ofSeconds(10));

        // No event_id: the server generates both ids.
        CompletableFuture<HttpResponse<String>> pending =
                postAsync("[{\"source\":\"a\",\"type\":\"t\"},{\"source\":\"b\",\"type\":\"t\"}]");
        awaitSends(2);
        producer.completeNext();
        producer.errorNext(new RuntimeException("broker unavailable"));

        HttpResponse<String> response = pending.get(5, TimeUnit.SECONDS);
        assertThat(response.statusCode()).isEqualTo(503);
        JsonNode body = JSON.readTree(response.body());
        assertThat(body.get("failed").asInt()).isEqualTo(1);
        assertThat(body.get("event_ids")).hasSize(2);
        String first = body.get("event_ids").get(0).asText();
        String second = body.get("event_ids").get(1).asText();
        assertThat(first).isNotBlank();
        assertThat(first).isNotEqualTo(second);
        assertThat(body.get("failed_event_ids")).hasSize(1);
        assertThat(body.get("failed_event_ids").get(0).asText()).isEqualTo(second);
    }

    @Test
    void nullEventsAreRejectedAs400() throws Exception {
        start(true, 10, 1 << 20, Duration.ofSeconds(5));

        for (String body : new String[] {"null", "[null]", "[{\"source\":\"a\",\"type\":\"t\"}, null]"}) {
            HttpResponse<String> response = post(body);
            // JUnit's assertEquals for the message: AssertJ's chained .as(...) trips the IDE's null analysis.
            assertEquals(400, response.statusCode(), body);
            assertThat(java.util.Objects.requireNonNull(response.body())).contains("malformed_json", "not null");
        }
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void returns503WhenBrokerDoesNotAcknowledgeInTime() throws Exception {
        start(false, 10, 1 << 20, Duration.ofMillis(200));

        HttpResponse<String> response = post("{\"source\":\"a\",\"type\":\"t\"}");

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).contains("timed out");
    }
}
