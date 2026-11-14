package io.lakehouse.ingest;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/** Application metrics. Names map to Prometheus as e.g. {@code ingest_requests_total}. */
public final class IngestMetrics {

    private static final Duration[] SLOS = {
        Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50),
        Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
        Duration.ofMillis(2500), Duration.ofSeconds(5)
    };

    private final MeterRegistry registry;
    private final Timer produceLatency;
    private final Timer requestLatency;

    public IngestMetrics(MeterRegistry registry, Semaphore permits, int maxInFlight) {
        this.registry = registry;
        this.produceLatency = Timer.builder("ingest.produce.latency")
                .description("Time from send() to broker acknowledgement (acks=all)")
                .serviceLevelObjectives(SLOS)
                .register(Objects.requireNonNull(registry));
        this.requestLatency = Timer.builder("ingest.request.latency")
                .description("End-to-end HTTP request handling time for /v1/events")
                .serviceLevelObjectives(SLOS)
                .register(Objects.requireNonNull(registry));
        Gauge.builder("ingest.inflight", permits, p -> maxInFlight - p.availablePermits())
                .description("Events sent to Kafka and not yet acknowledged")
            .register(Objects.requireNonNull(registry));
        Gauge.builder("ingest.inflight.max", () -> maxInFlight)
                .description("Back-pressure limit on in-flight events")
            .register(Objects.requireNonNull(registry));
    }

    public void request(String outcome) {
        Counter.builder("ingest.requests")
                .description("HTTP requests to /v1/events by outcome")
                .tag("outcome", Objects.requireNonNull(outcome))
                .register(Objects.requireNonNull(registry))
                .increment();
    }

    public void events(String outcome, int count) {
        Counter.builder("ingest.events")
                .description("Events by outcome (published, invalid, rejected, failed)")
                .tag("outcome", Objects.requireNonNull(outcome))
                .register(Objects.requireNonNull(registry))
                .increment(count);
    }

    public Timer produceLatency() {
        return produceLatency;
    }

    public Timer requestLatency() {
        return requestLatency;
    }
}
