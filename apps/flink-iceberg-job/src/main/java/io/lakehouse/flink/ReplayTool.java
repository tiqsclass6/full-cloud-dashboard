package io.lakehouse.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Sends records the lakehouse does not have back through the ingestion API, with their original event_id.
 *
 * <pre>
 *   ReplayTool missing --partition 3 --offset 276204 [--count 1] [--apply]
 *   ReplayTool undecodable [--since 2026-11-19T00:00:00Z] [--partition 1 --offset 190353] [--apply]
 * </pre>
 *
 * Without --apply it only prints what it would do. One JSON line per offset, then a summary line.
 * Environment: as the job (Kafka, registry, catalog, DEDUPE_TTL_MINUTES), plus INGEST_URL.
 */
public final class ReplayTool {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int BATCH = 200;

    private final JobConfig config;
    private final Catalog catalog;
    private final ConfluentAvroRowDeserializer avro;
    private final String ingestUrl;

    private ReplayTool(JobConfig config, Catalog catalog, String ingestUrl) {
        this.config = config;
        this.catalog = catalog;
        this.avro = new ConfluentAvroRowDeserializer(config.schemaRegistryUrl(), IcebergTables.initialSchema());
        this.ingestUrl = ingestUrl;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = options(args);
        String mode = args.length > 0 ? args[0] : "";
        if (!mode.equals("missing") && !mode.equals("undecodable")
                || mode.equals("missing") && (!opts.containsKey("partition") || !opts.containsKey("offset"))) {
            System.err.println("usage: ReplayTool missing --partition P --offset O [--count N] [--apply]\n"
                    + "       ReplayTool undecodable [--since ISO-8601] [--partition P --offset O] [--apply]");
            System.exit(2);
        }
        Map<String, String> env = System.getenv();
        JobConfig config = JobConfig.fromEnv(env);
        Catalog catalog = IcebergTables.openCatalog(config);
        try {
            ReplayTool tool = new ReplayTool(config, catalog,
                    env.getOrDefault("INGEST_URL", "http://ingestion-service.apps.svc:8080/v1/events"));
            boolean apply = opts.containsKey("apply");
            int failed = mode.equals("missing")
                    ? tool.missing(Integer.parseInt(opts.get("partition")), Long.parseLong(opts.get("offset")),
                            Integer.parseInt(opts.getOrDefault("count", "1")), apply)
                    : tool.undecodable(opts.get("since"), opts.get("partition"), opts.get("offset"), apply);
            System.exit(failed == 0 ? 0 : 1);
        } finally {
            IcebergTables.close(catalog);
        }
    }

    private int missing(int partition, long first, int count, boolean apply) throws Exception {
        long last = first + count - 1;
        Map<Long, ConsumerRecord<byte[], byte[]>> kafka = new HashMap<>();
        Map<Long, String> notInKafka = readKafka(partition, first, last, kafka);
        Table events = table(config.icebergTable());
        Table skipped = table(config.icebergSkippedTable());
        Set<Long> stored = offsets(events, partition, first, last, null);
        Map<Long, String> skippedReasons = new HashMap<>();
        offsets(skipped, partition, first, last, skippedReasons);
        long since = kafka.values().stream().mapToLong(ConsumerRecord::timestamp).min().orElse(0);
        Set<String> replayed = markers(events, since);

        Map<Long, GenericRecord> decoded = new HashMap<>();
        Map<Long, String> errors = new HashMap<>();
        for (Map.Entry<Long, ConsumerRecord<byte[], byte[]>> e : kafka.entrySet()) {
            decode(e.getValue().value(), e.getKey(), decoded, errors);
        }
        Set<String> storedIds = storedEventIds(events, decoded.values());

        List<Replay.Facts> facts = new ArrayList<>();
        for (long o = first; o <= last; o++) {
            ConsumerRecord<byte[], byte[]> r = kafka.get(o);
            GenericRecord record = decoded.get(o);
            facts.add(new Replay.Facts(partition, o, r == null ? 0 : r.timestamp(), stored.contains(o),
                    skippedReasons.get(o), replayed.contains(partition + ":" + o),
                    r == null ? notInKafka.getOrDefault(o, "not read") : null, record, errors.get(o),
                    record != null && storedIds.contains(record.get("event_id").toString())));
        }
        Instant now = Instant.now();
        return act("missing", facts, f -> Replay.decideMissing(f, now, config.dedupeTtl()), apply);
    }

    private int undecodable(String since, String partition, String offset, boolean apply) throws Exception {
        Table events = table(config.icebergTable());
        Table skipped = table(config.icebergSkippedTable());
        Expression filter = Expressions.equal("reason", SkippedRecords.UNDECODABLE);
        if (since != null) {
            filter = Expressions.and(filter, Expressions.greaterThanOrEqual(IcebergTables.KAFKA_TIMESTAMP,
                    DateTimeUtil.microsFromInstant(Instant.parse(since))));
        }
        if (partition != null && offset != null) {
            filter = Expressions.and(filter, Expressions.and(
                    Expressions.equal(IcebergTables.KAFKA_PARTITION, Integer.parseInt(partition)),
                    Expressions.equal(IcebergTables.KAFKA_OFFSET, Long.parseLong(offset))));
        }
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> it = IcebergGenerics.read(skipped).where(filter)
                .select(IcebergTables.KAFKA_PARTITION, IcebergTables.KAFKA_OFFSET, IcebergTables.KAFKA_TIMESTAMP,
                        "payload").build()) {
            it.forEach(r -> rows.add(r.copy()));
        }
        rows.sort((a, b) -> Long.compare(millis(a), millis(b)));
        long earliest = rows.isEmpty() ? 0 : millis(rows.get(0));
        Set<String> replayed = markers(events, earliest);

        Map<Long, GenericRecord> decoded = new HashMap<>();
        Map<Long, String> errors = new HashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            decode(bytes(rows.get(i).getField("payload")), i, decoded, errors);
        }
        Set<String> storedIds = storedEventIds(events, decoded.values());
        List<Replay.Facts> facts = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Record r = rows.get(i);
            int p = (Integer) r.getField(IcebergTables.KAFKA_PARTITION);
            long o = (Long) r.getField(IcebergTables.KAFKA_OFFSET);
            GenericRecord record = decoded.get((long) i);
            facts.add(new Replay.Facts(p, o, millis(r), false, SkippedRecords.UNDECODABLE,
                    replayed.contains(p + ":" + o), null, record, errors.get((long) i),
                    record != null && storedIds.contains(record.get("event_id").toString())));
        }
        return act("undecodable", facts, Replay::decideUndecodable, apply);
    }

    /** Prints each decision, sends the replays when applying, then a summary; returns the number that failed. */
    private int act(String mode, List<Replay.Facts> facts, java.util.function.Function<Replay.Facts, Replay.Decision> decide,
            boolean apply) throws Exception {
        Map<String, Integer> outcomes = new TreeMap<>();
        List<Map<String, Object>> toSend = new ArrayList<>();
        for (Replay.Facts f : facts) {
            Replay.Decision d = decide.apply(f);
            outcomes.merge(d.outcome().name(), 1, Integer::sum);
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("partition", f.partition());
            line.put("offset", f.offset());
            line.put("outcome", d.outcome().name());
            if (d.detail() != null) {
                line.put("detail", d.detail());
            }
            if (f.record() != null) {
                line.put("event_id", f.record().get("event_id").toString());
            }
            System.out.println(JSON.writeValueAsString(line));
            if (d.outcome() == Replay.Outcome.REPLAY) {
                toSend.add(Replay.eventJson(f.record(),
                        Replay.marker(f.partition(), f.offset(), f.kafkaTimestampMs())));
            }
        }
        int sent = 0;
        int failed = 0;
        if (apply) {
            for (int i = 0; i < toSend.size(); i += BATCH) {
                List<Map<String, Object>> batch = toSend.subList(i, Math.min(toSend.size(), i + BATCH));
                if (post(batch)) {
                    sent += batch.size();
                } else {
                    failed += batch.size();
                }
            }
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("mode", mode);
        summary.put("applied", apply);
        summary.put("checked", facts.size());
        summary.put("outcomes", outcomes);
        summary.put("to_replay", toSend.size());
        summary.put("sent", sent);
        summary.put("failed", failed);
        System.out.println("SUMMARY " + JSON.writeValueAsString(summary));
        return failed;
    }

    /** The retry contract: 429, 503 or no reply means retry with the same ids; 400 means the request is wrong. */
    private boolean post(List<Map<String, Object>> events) throws Exception {
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        String body = JSON.writeValueAsString(events);
        for (int attempt = 1; attempt <= 20; attempt++) {
            try {
                HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(ingestUrl))
                        .timeout(Duration.ofSeconds(30)).header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    return true;
                }
                if (response.statusCode() != 429 && response.statusCode() != 503) {
                    System.err.println("ingestion API answered " + response.statusCode() + ": " + response.body());
                    return false;
                }
            } catch (IOException e) {
                System.err.println("ingestion API unreachable (attempt " + attempt + "): " + e);
            }
            Thread.sleep(Math.min(5000, 250L * attempt));
        }
        return false;
    }

    /** Reads [first, last] of one partition into {@code out}; returns why an offset could not be read. */
    private Map<Long, String> readKafka(int partition, long first, long last,
            Map<Long, ConsumerRecord<byte[], byte[]>> out) {
        Properties props = config.kafkaClientProperties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // No group: assign + seek needs only Read on the topic, which the Flink identity has.
        Map<Long, String> reasons = new HashMap<>();
        TopicPartition tp = new TopicPartition(config.topic(), partition);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(List.of(tp));
            long begin = consumer.beginningOffsets(List.of(tp)).get(tp);
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            for (long o = first; o <= last; o++) {
                if (o < begin) {
                    reasons.put(o, "expired from Kafka (earliest offset is " + begin + ")");
                } else if (o >= end) {
                    reasons.put(o, "beyond the end of the partition (" + end + ")");
                }
            }
            long from = Math.max(first, begin);
            long to = Math.min(last, end - 1);
            if (from > to) {
                return reasons;
            }
            consumer.seek(tp, from);
            long deadline = System.currentTimeMillis() + 60_000;
            long next = from;
            while (next <= to && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofSeconds(2)).records(tp)) {
                    if (r.offset() <= to) {
                        out.put(r.offset(), r);
                    }
                    next = r.offset() + 1;
                }
            }
            for (long o = from; o <= to; o++) {
                if (!out.containsKey(o)) {
                    reasons.put(o, "not returned by Kafka (a transaction marker or a timeout)");
                }
            }
        }
        return reasons;
    }

    private void decode(byte[] bytes, long key, Map<Long, GenericRecord> decoded, Map<Long, String> errors)
            throws IOException {
        try {
            decoded.put(key, avro.decodeRecord(bytes));
        } catch (ConfluentAvroRowDeserializer.PoisonRecordException e) {
            errors.put(key, e.getMessage());
        }
    }

    private Table table(String name) {
        return catalog.loadTable(TableIdentifier.of(config.icebergNamespace(), name));
    }

    /** Offsets in [first, last] of a partition present in the table; with {@code reasons}, also their reason. */
    private static Set<Long> offsets(Table table, int partition, long first, long last, Map<Long, String> reasons)
            throws IOException {
        Set<Long> found = new HashSet<>();
        Expression filter = Expressions.and(Expressions.equal(IcebergTables.KAFKA_PARTITION, partition),
                Expressions.and(Expressions.greaterThanOrEqual(IcebergTables.KAFKA_OFFSET, first),
                        Expressions.lessThanOrEqual(IcebergTables.KAFKA_OFFSET, last)));
        IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(table).where(filter);
        scan = reasons == null ? scan.select(IcebergTables.KAFKA_OFFSET) : scan.select(IcebergTables.KAFKA_OFFSET, "reason");
        try (CloseableIterable<Record> it = scan.build()) {
            for (Record r : it) {
                long o = (Long) r.getField(IcebergTables.KAFKA_OFFSET);
                found.add(o);
                if (reasons != null) {
                    reasons.put(o, String.valueOf(r.getField("reason")));
                }
            }
        }
        return found;
    }

    /** "partition:offset" of every replay marker on rows written since {@code sinceMs} (a replay comes later). */
    private static Set<String> markers(Table events, long sinceMs) throws IOException {
        Set<String> found = new HashSet<>();
        try (CloseableIterable<Record> it = IcebergGenerics.read(events)
                .where(Expressions.greaterThanOrEqual(IcebergTables.KAFKA_TIMESTAMP,
                        DateTimeUtil.microsFromInstant(Instant.ofEpochMilli(sinceMs))))
                .select("attributes").build()) {
            for (Record r : it) {
                if (r.getField("attributes") instanceof Map<?, ?> attributes) {
                    long[] m = Replay.parseMarker((String) attributes.get(Replay.MARKER));
                    if (m != null) {
                        found.add(m[0] + ":" + m[1]);
                    }
                }
            }
        }
        return found;
    }

    private static Set<String> storedEventIds(Table events, Iterable<GenericRecord> records) throws IOException {
        Set<String> ids = new HashSet<>();
        records.forEach(r -> ids.add(r.get("event_id").toString()));
        Set<String> found = new HashSet<>();
        if (ids.isEmpty()) {
            return found;
        }
        try (CloseableIterable<Record> it = IcebergGenerics.read(events)
                .where(Expressions.in("event_id", ids.toArray())).select("event_id").build()) {
            it.forEach(r -> found.add((String) r.getField("event_id")));
        }
        return found;
    }

    private static long millis(Record r) {
        return ((OffsetDateTime) r.getField(IcebergTables.KAFKA_TIMESTAMP)).toInstant().toEpochMilli();
    }

    private static byte[] bytes(Object value) {
        if (value instanceof ByteBuffer buffer) {
            byte[] out = new byte[buffer.remaining()];
            buffer.duplicate().get(out);
            return out;
        }
        return (byte[]) value;
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("--apply")) {
                opts.put("apply", "true");
            } else if (args[i].startsWith("--") && i + 1 < args.length) {
                opts.put(args[i].substring(2), args[++i]);
            } else {
                throw new IllegalArgumentException("unexpected argument " + args[i]);
            }
        }
        return opts;
    }
}
