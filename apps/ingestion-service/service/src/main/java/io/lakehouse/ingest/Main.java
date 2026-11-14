package io.lakehouse.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point. Configuration comes from environment variables (see {@link Config}). */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main() {}

    public static void main(String[] args) {
        IngestService service = new IngestService(Config.fromEnv(System.getenv()));
        Runtime.getRuntime().addShutdownHook(new Thread(service::close, "shutdown"));
        try {
            service.start();
        } catch (Exception | Error e) {
            // The management server's thread would keep a never-Ready pod alive; exit so Kubernetes restarts it.
            LOG.error("Startup failed; exiting so the platform can restart the service", e);
            System.exit(1);
        }
    }
}
