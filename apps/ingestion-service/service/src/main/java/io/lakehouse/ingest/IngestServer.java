package io.lakehouse.ingest;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** The ingest endpoint: one virtual thread per request, so blocking on Kafka acks needs no sized pool. */
public final class IngestServer {

    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public IngestServer(int port, IngestHandler handler) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 1024);
        server.createContext("/v1/events", handler);
        server.setExecutor(executor);
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(5);
        executor.shutdown();
    }

    public int port() {
        return server.getAddress().getPort();
    }
}
