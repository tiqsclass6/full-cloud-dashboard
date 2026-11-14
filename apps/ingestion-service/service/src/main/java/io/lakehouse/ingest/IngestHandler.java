package io.lakehouse.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** HTTP handler for {@code POST /v1/events}: accepts one event object or an array of events. */
public final class IngestHandler implements HttpHandler {

    private final EventIngestor ingestor;
    private final IngestMetrics metrics;
    private final int maxBodyBytes;
    private final ObjectMapper mapper = new ObjectMapper();

    public IngestHandler(EventIngestor ingestor, IngestMetrics metrics, int maxBodyBytes) {
        this.ingestor = ingestor;
        this.metrics = metrics;
        this.maxBodyBytes = maxBodyBytes;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        long start = System.nanoTime();
        int status = 500;
        try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("Allow", "POST");
                status = respond(exchange, 405, Map.of("error", "method_not_allowed"));
                metrics.request("method_not_allowed");
                return;
            }

            byte[] body = exchange.getRequestBody().readNBytes(maxBodyBytes + 1);
            if (body.length > maxBodyBytes) {
                status = respond(exchange, 413, Map.of("error", "payload_too_large", "max_bytes", maxBodyBytes));
                metrics.request("too_large");
                return;
            }

            List<EventRequest> events;
            try {
                events = parse(body);
            } catch (JsonProcessingException e) {
                status = respond(exchange, 400, Map.of("error", "malformed_json", "details", List.of(e.getOriginalMessage())));
                metrics.request("malformed");
                return;
            }

            EventIngestor.Result result = ingestor.ingest(events);
            switch (result) {
                case EventIngestor.Accepted accepted -> {
                    status = respond(exchange, 200, Map.of(
                            "accepted", accepted.eventIds().size(), "event_ids", accepted.eventIds()));
                    metrics.request("accepted");
                }
                case EventIngestor.Invalid invalid -> {
                    status = respond(exchange, 400, Map.of("error", "validation_failed", "details", invalid.errors()));
                    metrics.request("invalid");
                }
                case EventIngestor.Overloaded ignored -> {
                    exchange.getResponseHeaders().add("Retry-After", "1");
                    status = respond(exchange, 429, Map.of("error", "overloaded"));
                    metrics.request("overloaded");
                }
                case EventIngestor.Failed failed -> {
                    exchange.getResponseHeaders().add("Retry-After", "2");
                    status = respond(exchange, 503, Map.of(
                            "error", "publish_failed", "failed", failed.failedCount(), "reason", failed.reason(),
                            "event_ids", failed.eventIds(), "failed_event_ids", failed.failedEventIds()));
                    metrics.request("failed");
                }
            }
        } finally {
            metrics.requestLatency().record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
            if (status == 500) {
                metrics.request("error");
            }
        }
    }

    private List<EventRequest> parse(byte[] body) throws IOException {
        int i = 0;
        while (i < body.length && Character.isWhitespace(body[i])) {
            i++;
        }
        // readerFor, not readValue(body, X.class): the class literal makes the IDE's null analysis assume non-null.
        EventRequest[] events;
        if (i < body.length && body[i] == '[') {
            events = mapper.readerFor(EventRequest[].class).readValue(body);
        } else {
            EventRequest single = mapper.readerFor(EventRequest.class).readValue(body);
            events = new EventRequest[] {single};
        }
        // JSON null binds to a Java null, which List.of rejects.
        if (events == null || Arrays.asList(events).contains(null)) {
            throw new JsonMappingException((Closeable) null, "every event must be a JSON object, not null");
        }
        return List.of(events);
    }

    private int respond(HttpExchange exchange, int status, Object payload) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
        return status;
    }
}
