package io.lakehouse.ingest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validates and publishes events, completing once Kafka acknowledged all; a semaphore bounds in-flight events. */
public final class EventIngestor {

    private static final Logger LOG = LoggerFactory.getLogger(EventIngestor.class);

    public sealed interface Result permits Accepted, Invalid, Overloaded, Failed {}

    public record Accepted(List<String> eventIds) implements Result {}

    public record Invalid(List<String> errors) implements Result {}

    public record Overloaded() implements Result {}

    /** {@code eventIds}: every event's id in request order, so the source can retry exactly what failed. */
    public record Failed(int failedCount, String reason, List<String> eventIds, List<String> failedEventIds)
            implements Result {}

    private final Producer<String, byte[]> producer;
    private final ConfluentAvroEncoder encoder;
    private final EventSchema schema;
    private final EventValidator validator;
    private final IngestMetrics metrics;
    private final Semaphore permits;
    private final Config config;

    public EventIngestor(
            Producer<String, byte[]> producer,
            ConfluentAvroEncoder encoder,
            EventSchema schema,
            EventValidator validator,
            IngestMetrics metrics,
            Semaphore permits,
            Config config) {
        this.producer = producer;
        this.encoder = encoder;
        this.schema = schema;
        this.validator = validator;
        this.metrics = metrics;
        this.permits = permits;
        this.config = config;
    }

    public Result ingest(List<EventRequest> events) {
        if (events.isEmpty()) {
            return new Invalid(List.of("request contains no events"));
        }
        if (events.size() > config.maxBatchSize()) {
            return new Invalid(List.of("batch has " + events.size() + " events; the maximum is "
                    + config.maxBatchSize()));
        }

        List<String> errors = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            for (String problem : validator.validate(events.get(i))) {
                errors.add(events.size() == 1 ? problem : "events[" + i + "]: " + problem);
            }
        }
        if (!errors.isEmpty()) {
            metrics.events("invalid", events.size());
            return new Invalid(errors);
        }

        int n = events.size();
        if (!permits.tryAcquire(n)) {
            metrics.events("rejected", n);
            return new Overloaded();
        }

        List<String> ids = new ArrayList<>(n);
        List<CompletableFuture<Void>> acks = new ArrayList<>(n);
        for (EventRequest event : events) {
            String id = java.util.Objects.requireNonNull(event.eventId(), "validated above");
            ids.add(id);
            GenericRecord record = schema.toRecord(event, id);
            CompletableFuture<Void> ack = new CompletableFuture<>();
            acks.add(ack);
            long start = System.nanoTime();
            try {
                producer.send(
                        new ProducerRecord<>(config.topic(), event.source(), encoder.encode(record)),
                        (metadata, exception) -> {
                            permits.release();
                            if (exception != null) {
                                ack.completeExceptionally(exception);
                            } else {
                                metrics.produceLatency().record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                                ack.complete(null);
                            }
                        });
            } catch (RuntimeException e) {
                // send() can throw synchronously (e.g. metadata timeout); the callback will not run.
                permits.release();
                ack.completeExceptionally(e);
            }
        }

        try {
            CompletableFuture.allOf(acks.toArray(new CompletableFuture<?>[0]))
                    .get(config.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return failed(ids, acks, "timed out waiting for broker acknowledgement");
        } catch (ExecutionException e) {
            LOG.warn("Publish failed: {}", e.getCause().toString());
            return failed(ids, acks, e.getCause().getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failed(ids, acks, "interrupted");
        }
        metrics.events("published", n);
        return new Accepted(ids);
    }

    private Failed failed(List<String> ids, List<CompletableFuture<Void>> acks, String reason) {
        List<String> failedIds = new ArrayList<>();
        for (int i = 0; i < acks.size(); i++) {
            CompletableFuture<Void> ack = acks.get(i);
            if (!ack.isDone() || ack.isCompletedExceptionally()) {
                failedIds.add(ids.get(i));
            }
        }
        metrics.events("failed", failedIds.size());
        // Acknowledged events stay in Kafka; a retry with the same ids is de-duplicated downstream.
        metrics.events("published", acks.size() - failedIds.size());
        return new Failed(failedIds.size(), reason, List.copyOf(ids), List.copyOf(failedIds));
    }
}
