package io.lakehouse.ingest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

/** Separate port for Prometheus scraping and Kubernetes probes, so ops traffic never competes with ingest. */
public final class ManagementServer {

    private final HttpServer server;

    public ManagementServer(int port, PrometheusMeterRegistry registry, BooleanSupplier ready) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 16);
        server.createContext("/metrics", exchange ->
                respond(exchange, 200, registry.scrape(), "text/plain; version=0.0.4; charset=utf-8"));
        server.createContext("/health/live", exchange -> respond(exchange, 200, "UP\n", "text/plain"));
        server.createContext("/health/ready", exchange ->
                respond(exchange, ready.getAsBoolean() ? 200 : 503, ready.getAsBoolean() ? "READY\n" : "NOT_READY\n", "text/plain"));
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(1);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
    }
}
