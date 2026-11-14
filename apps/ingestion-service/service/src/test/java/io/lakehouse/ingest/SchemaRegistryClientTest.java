package io.lakehouse.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SchemaRegistryClientTest {

    private HttpServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private String start(java.util.function.ToIntFunction<AtomicInteger> statusPerCall, String okBody,
            AtomicReference<String> seenBody, AtomicReference<String> seenContentType) throws IOException {
        AtomicInteger calls = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/subjects/events.raw-value/versions", exchange -> {
            seenBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            seenContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            int status = statusPerCall.applyAsInt(calls);
            byte[] body = (status == 200 ? okBody : "{\"message\":\"boom\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    void registersSchemaAndReturnsId() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        AtomicReference<String> contentType = new AtomicReference<>();
        String url = start(calls -> 200, "{\"id\":7}", body, contentType);

        int id = new SchemaRegistryClient(url).register("events.raw-value", "{\"type\":\"string\"}");

        assertThat(id).isEqualTo(7);
        assertThat(contentType.get()).isEqualTo("application/vnd.schemaregistry.v1+json");
        assertThat(body.get()).isEqualTo("{\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}");
    }

    @Test
    void failsOnServerError() throws Exception {
        String url = start(calls -> 500, "", new AtomicReference<>(), new AtomicReference<>());

        assertThatExceptionOfType(IOException.class)
            .isThrownBy(() -> new SchemaRegistryClient(url).register("events.raw-value", "{}"))
            .withMessageContaining("HTTP 500");
    }

    @Test
    void retriesUntilRegistryIsAvailable() throws Exception {
        String url = start(calls -> calls.incrementAndGet() < 3 ? 503 : 200, "{\"id\":9}",
                new AtomicReference<>(), new AtomicReference<>());

        int id = new SchemaRegistryClient(url)
                .registerWithRetry("events.raw-value", "{}", 5, Duration.ofMillis(10));

        assertThat(id).isEqualTo(9);
    }

    @Test
    void givesUpAfterAllAttempts() throws Exception {
        String url = start(calls -> 503, "", new AtomicReference<>(), new AtomicReference<>());

        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> new SchemaRegistryClient(url)
                        .registerWithRetry("events.raw-value", "{}", 2, Duration.ofMillis(10)))
                .withMessageContaining("after 2 attempts");
    }
}
