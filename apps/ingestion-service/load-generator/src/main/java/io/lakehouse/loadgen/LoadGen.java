package io.lakehouse.loadgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/** Open-loop load generator: latency counts from the intended send time, which avoids coordinated omission. */
public final class LoadGen {

    record Options(String url, int eventsPerSecond, int durationSeconds, int warmupSeconds, int workers,
            int batchSize, int sources) {

        static Options parse(String[] args) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i++) {
                if (!args[i].startsWith("--") || i + 1 >= args.length) {
                    throw new IllegalArgumentException("expected --name value pairs, got: " + args[i]);
                }
                values.put(args[i].substring(2), args[++i]);
            }
            Options options = new Options(
                    values.getOrDefault("url", "http://localhost:8082/v1/events"),
                    Integer.parseInt(values.getOrDefault("rate", "1000")),
                    Integer.parseInt(values.getOrDefault("duration", "60")),
                    Integer.parseInt(values.getOrDefault("warmup", "10")),
                    Integer.parseInt(values.getOrDefault("workers", "64")),
                    Integer.parseInt(values.getOrDefault("batch", "10")),
                    Integer.parseInt(values.getOrDefault("sources", "100")));
            if (options.eventsPerSecond() <= 0 || options.batchSize() <= 0 || options.workers() <= 0
                    || options.durationSeconds() <= options.warmupSeconds()) {
                throw new IllegalArgumentException(
                        "rate, batch and workers must be positive and duration must exceed warmup");
            }
            return options;
        }

        /** Requests per second across all workers. */
        double requestsPerSecond() {
            return (double) eventsPerSecond / batchSize;
        }

        /** Gap between two consecutive requests of one worker, in nanoseconds. */
        long workerIntervalNanos() {
            return (long) (1_000_000_000d * workers / requestsPerSecond());
        }
    }

    private LoadGen() {}

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        System.out.printf("loadgen: %d events/s (%d per request, %.1f req/s), %d workers, %ds (%ds warmup) -> %s%n",
                options.eventsPerSecond(), options.batchSize(), options.requestsPerSecond(), options.workers(),
                options.durationSeconds(), options.warmupSeconds(), options.url());

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        Recorder latencies = new Recorder(TimeUnit.SECONDS.toNanos(30), 3);
        Map<String, LongAdder> outcomes = new ConcurrentHashMap<>();
        LongAdder measuredRequests = new LongAdder();

        long startNanos = System.nanoTime();
        long warmupEnd = startNanos + TimeUnit.SECONDS.toNanos(options.warmupSeconds());
        long end = startNanos + TimeUnit.SECONDS.toNanos(options.durationSeconds());
        long interval = options.workerIntervalNanos();

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> workers = new ArrayList<>();
            for (int w = 0; w < options.workers(); w++) {
                // Stagger workers evenly across one interval so load is smooth, not bursty.
                long firstSend = startNanos + interval * w / options.workers();
                workers.add(pool.submit(() -> run(client, options, firstSend, interval, warmupEnd, end, latencies,
                        outcomes, measuredRequests)));
            }
            for (Future<?> worker : workers) {
                worker.get();
            }
        }

        report(options, latencies.getIntervalHistogram(), outcomes, measuredRequests.sum());
    }

    private static void run(HttpClient client, Options options, long firstSend, long interval, long warmupEnd,
            long end, Recorder latencies, Map<String, LongAdder> outcomes, LongAdder measuredRequests) {
        URI uri = URI.create(options.url());
        for (long intended = firstSend; intended < end; intended += interval) {
            long wait = intended - System.nanoTime();
            if (wait > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(wait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            String outcome;
            try {
                HttpResponse<Void> response = client.send(
                        HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(body(options)))
                                .build(),
                        HttpResponse.BodyHandlers.discarding());
                outcome = "http_" + response.statusCode();
            } catch (java.io.IOException e) {
                outcome = "error_" + e.getClass().getSimpleName();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            long done = System.nanoTime();
            if (intended >= warmupEnd) {
                // Measured from the intended send time, so queueing behind a slow response counts.
                latencies.recordValue(Math.min(done - intended, TimeUnit.SECONDS.toNanos(30)));
                outcomes.computeIfAbsent(outcome, k -> new LongAdder()).increment();
                measuredRequests.increment();
            }
        }
    }

    static String body(Options options) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder body = new StringBuilder(options.batchSize() * 110);
        body.append('[');
        for (int i = 0; i < options.batchSize(); i++) {
            if (i > 0) {
                body.append(',');
            }
            body.append("{\"source\":\"load-").append(random.nextInt(options.sources()))
                    .append("\",\"type\":\"load.test\",\"value\":").append(random.nextDouble())
                    .append(",\"attributes\":{\"region\":\"us-east-1\",\"run\":\"loadgen\"}}");
        }
        return body.append(']').toString();
    }

    private static void report(Options options, Histogram h, Map<String, LongAdder> outcomes, long requests)
            throws Exception {
        double measuredSeconds = options.durationSeconds() - options.warmupSeconds();
        long ok = outcomes.getOrDefault("http_200", new LongAdder()).sum();
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("target_events_per_second", options.eventsPerSecond());
        summary.put("measured_seconds", measuredSeconds);
        summary.put("requests", requests);
        summary.put("achieved_events_per_second", Math.round(ok * options.batchSize() / measuredSeconds));
        Map<String, Long> counts = new LinkedHashMap<>();
        outcomes.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(e -> counts.put(e.getKey(), e.getValue().sum()));
        summary.put("outcomes", counts);
        Map<String, Double> ms = new LinkedHashMap<>();
        ms.put("p50", h.getValueAtPercentile(50) / 1e6);
        ms.put("p90", h.getValueAtPercentile(90) / 1e6);
        ms.put("p99", h.getValueAtPercentile(99) / 1e6);
        ms.put("p99.9", h.getValueAtPercentile(99.9) / 1e6);
        ms.put("max", h.getMaxValue() / 1e6);
        summary.put("latency_ms", ms);
        System.out.println("RESULT " + new ObjectMapper().writeValueAsString(summary));
    }
}
