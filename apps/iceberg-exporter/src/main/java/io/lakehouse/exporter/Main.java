package io.lakehouse.exporter;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Inspects the configured tables on an interval and serves Prometheus metrics; a failing table is marked down. */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) throws IOException {
        ExporterConfig config = ExporterConfig.fromEnv(System.getenv());
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Catalog catalog = CatalogUtil.loadCatalog(config.catalogImpl(), "lakehouse", config.catalogProperties(), null);

        Map<String, TableMetrics> metrics = new LinkedHashMap<>();
        for (String table : config.tables()) {
            metrics.put(table, new TableMetrics(registry, table));
        }
        AtomicBoolean firstRunDone = new AtomicBoolean(false);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "inspect");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            for (Map.Entry<String, TableMetrics> entry : metrics.entrySet()) {
                long start = System.nanoTime();
                try {
                    TableHealth health = TableHealth.inspect(
                            catalog.loadTable(TableIdentifier.parse(entry.getKey())), config.smallFileBytes());
                    entry.getValue().record(health, System.nanoTime() - start);
                } catch (RuntimeException e) {
                    entry.getValue().failed();
                    LOG.warn("inspection of {} failed: {}", entry.getKey(), e.toString());
                }
            }
            firstRunDone.set(true);
        }, 0, config.interval().toSeconds(), TimeUnit.SECONDS);

        HttpServer server = HttpServer.create(new InetSocketAddress(config.managementPort()), 16);
        server.createContext("/metrics", exchange ->
                respond(exchange, 200, registry.scrape(), "text/plain; version=0.0.4; charset=utf-8"));
        server.createContext("/health/live", exchange -> respond(exchange, 200, "UP\n", "text/plain"));
        server.createContext("/health/ready", exchange -> respond(exchange, firstRunDone.get() ? 200 : 503,
                firstRunDone.get() ? "READY\n" : "NOT_READY\n", "text/plain"));
        server.start();
        LOG.info("iceberg-exporter: {} table(s) every {}s, metrics on :{}", config.tables().size(),
                config.interval().toSeconds(), config.managementPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(1);
            scheduler.shutdownNow();
        }));
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
