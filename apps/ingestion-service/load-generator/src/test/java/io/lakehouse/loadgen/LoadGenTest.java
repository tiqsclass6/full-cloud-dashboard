package io.lakehouse.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class LoadGenTest {

    @Test
    void parsesOptionsWithDefaults() {
        LoadGen.Options options = LoadGen.Options.parse(new String[] {"--rate", "5000", "--batch", "50"});

        assertThat(options.eventsPerSecond()).isEqualTo(5000);
        assertThat(options.batchSize()).isEqualTo(50);
        assertThat(options.requestsPerSecond()).isEqualTo(100.0);
        assertThat(options.workers()).isEqualTo(64);
    }

    @Test
    void spacesRequestsPerWorkerToHitTheTargetRate() {
        // 1000 events/s in batches of 10 = 100 req/s over 10 workers = 10 req/s each = 100 ms apart.
        LoadGen.Options options = LoadGen.Options.parse(
                new String[] {"--rate", "1000", "--batch", "10", "--workers", "10"});

        assertThat(options.workerIntervalNanos()).isEqualTo(100_000_000L);
    }

    @Test
    void rejectsInvalidOptions() {
        assertThatThrownBy(() -> LoadGen.Options.parse(new String[] {"--rate"})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LoadGen.Options.parse(new String[] {"--duration", "5", "--warmup", "10"}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void generatesValidBatchBodies() throws Exception {
        LoadGen.Options options = LoadGen.Options.parse(new String[] {"--batch", "3", "--sources", "5"});

        JsonNode body = new ObjectMapper().readTree(LoadGen.body(options));

        assertThat(body.isArray()).isTrue();
        assertThat(body).hasSize(3);
        assertThat(body.get(0).get("source").asText()).startsWith("load-");
        assertThat(body.get(0).get("type").asText()).isEqualTo("load.test");
        // Every event carries its own UUID (the service rejects events without one).
        String first = body.get(0).get("event_id").asText();
        assertThat(java.util.UUID.fromString(first).toString()).isEqualTo(first);
        assertThat(body.get(1).get("event_id").asText()).isNotEqualTo(first);
    }
}
